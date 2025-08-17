const std = @import("std");

const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const lsm = @import("lib.zig");
const mtbl = @import("memtable.zig");
const opt = @import("opts.zig");
const sst = @import("sstable.zig");
const store = @import("store.zig");
const tm = @import("tablemap.zig");

const atomic = std.atomic;
const math = std.math;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const AtomicValue = atomic.Value;
const Mutex = std.Thread.Mutex;
const Order = math.Order;

const Iterator = iter.Iterator;
const MergeIterator = iter.MergeIterator;
const ScanIterator = iter.ScanIterator;
const KV = keyvalue.KV;
const Memtable = mtbl.Memtable;
const Opts = opt.Opts;
const SSTable = sst.SSTable;
const SSTableStore = store.SSTableStore;

pub const Database = struct {
    capacity: usize,
    mtable: AtomicValue(*Memtable),
    opts: Opts,
    mtables_mutex: Mutex,
    mtables: std.ArrayList(*Memtable),
    sstables: *SSTableStore,

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        var new_id_buf: [64]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.nanoTimestamp()});

        const mtable = Memtable.init(alloc, new_id) catch |err| {
            std.log.err("unable to init memtable {s}", .{@errorName(err)});
            return err;
        };
        errdefer {
            mtable.deinit();
            alloc.destroy(mtable);
        }

        const mtables = std.ArrayList(*Memtable).init(alloc);
        errdefer mtables.deinit();

        const sstables = try alloc.create(SSTableStore);
        sstables.* = try SSTableStore.init(alloc, opts);

        const capacity = opts.sst_capacity;

        const db = alloc.create(Self) catch |err| {
            std.log.err("unable to allocate db {s}", .{@errorName(err)});
            return err;
        };

        db.* = .{
            .capacity = capacity,
            .mtable = AtomicValue(*Memtable).init(mtable),
            .mtables = mtables,
            .mtables_mutex = Mutex{},
            .opts = opts,
            .sstables = sstables,
        };

        return db;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        self.mtables_mutex.lock();
        for (self.mtables.items) |mtable| {
            mtable.deinit();
            alloc.destroy(mtable);
        }
        self.mtables.deinit();
        self.mtables_mutex.unlock();

        var mtable = self.mtable.load(.seq_cst);
        mtable.deinit();
        alloc.destroy(mtable);

        self.sstables.deinit(alloc);
        alloc.destroy(self.sstables);

        self.* = undefined;
    }

    pub fn open(self: *Self, alloc: Allocator) !void {
        self.sstables.open(alloc) catch |err| {
            std.log.err("not able to open sstables data directory {s}", .{@errorName(err)});
            return err;
        };

        var count: usize = 0;
        for (0..self.sstables.num_levels) |lvl| {
            for (self.sstables.get(lvl)) |sstable| {
                var siter = try sstable.iterator(alloc);
                defer siter.deinit();

                const nxt_table = try Memtable.init(alloc, sstable.id);
                while (siter.next()) |nxt| {
                    const k = try nxt.clone(alloc);
                    try nxt_table.put(k);
                    count += 1;
                }

                nxt_table.isFlushed.store(true, .seq_cst);
                nxt_table.mutable.store(false, .seq_cst);

                try self.mtables.append(nxt_table);
            }
        }

        std.log.info(
            "database opened @ {s} w/ {d} warm tables and {d} total keys",
            .{ self.opts.data_dir, self.mtables.items.len, count },
        );
    }

    pub fn read(self: *Self, key: []const u8) !?KV {
        var latest_kv: ?KV = null;
        var latest_timestamp: i128 = std.math.minInt(i128);

        const hot_table = self.mtable.load(.acquire);
        if (try hot_table.get(key)) |kv| {
            latest_kv = kv;
            latest_timestamp = kv.timestamp;
        }

        var stack_snapshot: [16]*Memtable = undefined;
        var snapshot_tables: []*Memtable = undefined;
        var table_count: usize = 0;

        {
            self.mtables_mutex.lock();
            defer self.mtables_mutex.unlock();

            table_count = self.mtables.items.len;
            if (table_count <= stack_snapshot.len) {
                snapshot_tables = stack_snapshot[0..table_count];
                @memcpy(snapshot_tables, self.mtables.items);
            } else {
                // TODO: Update the allocator here
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const temp_alloc = arena.allocator();

                snapshot_tables = temp_alloc.alloc(*Memtable, table_count) catch |err| {
                    std.log.err("Failed to allocate snapshot tables array {s}", .{@errorName(err)});
                    return latest_kv;
                };
                @memcpy(snapshot_tables, self.mtables.items);
            }
        }

        var i = table_count;
        while (i > 0) {
            i -= 1;
            const table = snapshot_tables[i];
            if (try table.get(key)) |kv| {
                if (latest_kv == null or kv.timestamp > latest_timestamp) {
                    latest_timestamp = kv.timestamp;
                    latest_kv = kv;
                }
            }
        }

        return latest_kv;
    }

    const MergeIteratorWrapper = struct {
        alloc: Allocator,
        merger: *MergeIterator(KV, keyvalue.compare),
        iter: Iterator(KV),

        pub fn deinit(ctx: *anyopaque) void {
            var wrapper: *@This() = @ptrCast(@alignCast(ctx));
            wrapper.iter.deinit();
            wrapper.alloc.destroy(wrapper.merger);
            wrapper.alloc.destroy(wrapper);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            var wrapper: *@This() = @ptrCast(@alignCast(ctx));
            return wrapper.iter.next();
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        var merger = try alloc.create(MergeIterator(KV, keyvalue.compare));
        merger.* = try MergeIterator(KV, keyvalue.compare).init(alloc);

        const hot_table = self.mtable.load(.seq_cst);
        if (hot_table.size() > 0) {
            const hot_iter = try hot_table.iterator(alloc);
            try merger.add(hot_iter);
        }

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const temp_alloc = arena.allocator();

        var snapshot_tables: []*Memtable = undefined;
        var table_count: usize = 0;

        {
            self.mtables_mutex.lock();
            defer self.mtables_mutex.unlock();

            table_count = self.mtables.items.len;
            if (table_count > 0) {
                snapshot_tables = temp_alloc.alloc(*Memtable, table_count) catch |err| {
                    std.log.err("Failed to allocate snapshot tables for iterator {s}", .{@errorName(err)});
                    return error.OutOfMemory;
                };
                @memcpy(snapshot_tables, self.mtables.items);
            } else {
                snapshot_tables = &[_]*Memtable{};
            }
        }

        const warm_tables = snapshot_tables[0..table_count];

        for (warm_tables) |mtable| {
            const warm_iter = try mtable.iterator(alloc);
            try merger.add(warm_iter);
        }

        const wrapper = try alloc.create(MergeIteratorWrapper);
        errdefer alloc.destroy(wrapper);

        wrapper.* = .{
            .alloc = alloc,
            .iter = merger.iterator(),
            .merger = merger,
        };

        return Iterator(KV).init(wrapper, MergeIteratorWrapper.next, MergeIteratorWrapper.deinit);
    }

    const ScanWrapper = struct {
        alloc: Allocator,
        scanner: *ScanIterator(KV, keyvalue.userKeyCompare),
        iterator: Iterator(KV),

        const Wrapper = @This();

        pub fn deinit(ctx: *anyopaque) void {
            const sw: *Wrapper = @ptrCast(@alignCast(ctx));
            sw.iterator.deinit();
            if (sw.scanner.start) |start| {
                @constCast(&start).deinit(sw.alloc);
            }
            if (sw.scanner.end) |end| {
                @constCast(&end).deinit(sw.alloc);
            }
            sw.alloc.destroy(sw.scanner);
            sw.alloc.destroy(sw);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const sw: *Wrapper = @ptrCast(@alignCast(ctx));
            return sw.iterator.next();
        }
    };

    /// Creates a scan iterator that returns items with keys
    /// greater than start_key and less than or equal to end_key
    pub fn scan(
        self: *Self,
        alloc: Allocator,
        start_key: []const u8,
        end_key: []const u8,
    ) !Iterator(KV) {
        const start = try KV.initOwned(alloc, start_key, "");
        const end = try KV.initOwned(alloc, end_key, "");

        const base_iter = try self.iterator(alloc);

        const si = try alloc.create(ScanIterator(KV, keyvalue.userKeyCompare));
        si.* = ScanIterator(KV, keyvalue.userKeyCompare).init(base_iter, start, end);

        const wrapper = try alloc.create(ScanWrapper);
        errdefer alloc.destroy(wrapper);

        wrapper.* = .{
            .alloc = alloc,
            .iterator = si.iterator(),
            .scanner = si,
        };

        return Iterator(KV).init(wrapper, ScanWrapper.next, ScanWrapper.deinit);
    }

    pub fn write(self: *Self, kv: KV) anyerror!void {
        if (kv.key.len == 0) {
            return error.InvalidKeyData;
        }

        var mtable = self.mtable.load(.seq_cst);
        while (true) {
            mtable = self.mtable.load(.seq_cst);
            mtable.put(kv) catch |err| switch (err) {
                error.MemtableImmutable => continue,
                else => return err,
            };
            break;
        }
    }

    pub fn memtableCount(self: *Self) usize {
        self.mtables_mutex.lock();
        defer self.mtables_mutex.unlock();

        var count: usize = 0;
        for (self.mtables.items) |table| {
            if (!table.flushed()) {
                count += 1;
            }
        }

        return count;
    }

    pub fn freeze(self: *Self, alloc: Allocator, mtable: *Memtable) !void {
        if (mtable.frozen()) return;

        mtable.freeze();

        var new_id_buf: [64]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.nanoTimestamp()});

        const nxt_table = try Memtable.init(alloc, new_id);

        try self.mtables.append(mtable);

        self.mtable.store(nxt_table, .seq_cst);
    }

    pub fn flush(self: *Self, alloc: Allocator) void {
        const hot_table = self.mtable.load(.acquire);
        self.freeze(alloc, hot_table) catch |err| {
            std.log.err("db freeze failed {s}", .{@errorName(err)});
            return;
        };
        self.xflush(alloc) catch |err| {
            std.log.err("db flush failed {s}", .{@errorName(err)});
        };
    }

    pub fn xflush(self: *Self, alloc: Allocator) !void {
        self.mtables_mutex.lock();
        defer self.mtables_mutex.unlock();

        if (self.mtables.items.len == 0) {
            std.log.debug("nothing to flush", .{});
            return error.NothingToFlush;
        }

        for (self.mtables.items) |table| {
            if (!table.flushed()) {
                self.sstables.flush(alloc, table) catch |err| {
                    std.log.err(
                        "sstables not able to flush mtable {s} {s}",
                        .{ table.getId(), @errorName(err) },
                    );
                    return error.FlushFailed;
                };
            }
        }
    }
};

test Database {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // given
    var expected = try KV.initOwned(alloc, "__key__", "__value__");
    defer expected.deinit(alloc);

    // when
    try db.write(expected);

    // then
    const actual = try db.read(expected.key);

    try testing.expectEqualStrings(expected.value, actual.?.value);
}

test "basic functionality with many items" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const testDir = testing.tmpDir(.{});
    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(dir_name);
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer {
        db.deinit(alloc);
        alloc.destroy(db);
    }

    // given
    const keys = [_][]const u8{ "__key_c__", "__key_b__", "__key_d__", "__key_a__", "__key_e__" };
    const values = [_][]const u8{ "__value_c__", "__value_b__", "__value_d__", "__value_a__", "__value_e__" };

    // when
    for (keys, values) |key, value| {
        var expected = try KV.initOwned(alloc, key, value);
        defer expected.deinit(alloc);

        try db.write(expected);
    }

    // then
    for (keys, values) |key, expected_value| {
        const actual = try db.read(key);
        try testing.expectEqualStrings(expected_value, actual.?.value);
    }

    // then
    var it = try db.iterator(alloc);
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |_| {
        count += 1;
    }

    try testing.expectEqual(keys.len, count);

    var items = std.ArrayList(KV).init(alloc);
    defer items.deinit();

    var scan_iter = try db.scan(alloc, "__key_b__", "__key_d__");
    defer scan_iter.deinit();

    while (scan_iter.next()) |nxt| {
        try items.append(nxt);
    }

    try testing.expectEqual(3, items.items.len);
}

test "write with empty key returns error" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // Create a KV with empty key directly to bypass KV validation
    const kv = KV{
        .key = "",
        .value = "some_value",
        .raw_bytes = "",
        .timestamp = std.time.nanoTimestamp(),
        .ownership = .borrowed,
    };

    // when/then
    try testing.expectError(error.InvalidKeyData, db.write(kv));
}

test "memtableCount returns correct count" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // initially should be 0
    try testing.expectEqual(@as(usize, 0), db.memtableCount());

    // write some data and freeze to create memtables
    var kv = try KV.initOwned(alloc, "test_key", "test_value");
    defer kv.deinit(alloc);

    try db.write(kv);

    const hot_table = db.mtable.load(.seq_cst);
    try db.freeze(alloc, hot_table);

    // should now have 1 unflushed memtable
    try testing.expectEqual(@as(usize, 1), db.memtableCount());
}

test "flush with no memtables" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // xflush should return error when no memtables to flush
    try testing.expectError(error.NothingToFlush, db.xflush(alloc));
}

test "read returns null for non-existent key" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // when
    const result = try db.read("non_existent_key");

    // then
    try testing.expect(result == null);
}

test "freeze already frozen memtable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // write some data
    var kv = try KV.initOwned(alloc, "test_key", "test_value");
    defer kv.deinit(alloc);
    try db.write(kv);

    const hot_table = db.mtable.load(.seq_cst);

    // freeze once
    try db.freeze(alloc, hot_table);

    // freeze again - should be no-op
    try db.freeze(alloc, hot_table);

    // should still work without error
    try testing.expect(hot_table.frozen());
}
