const std = @import("std");

const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const lsm = @import("lib.zig");
const mtbl = @import("memtable.zig");
const opt = @import("opts.zig");
const sst = @import("sstable.zig");
const store = @import("store.zig");
const tm = @import("tablemap.zig");
const log = @import("wal.zig");

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
const WAL = log.WAL;

var tsa: std.heap.ThreadSafeAllocator = .{
    .child_allocator = std.heap.c_allocator,
};
var allocator = tsa.allocator();

pub const Database = struct {
    capacity: usize,

    opts: Opts,

    // in-memory
    memory: AtomicValue(*Memtable),

    // on disk
    wal: *WAL,
    io: *SSTableStore,

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        var new_id_buf: [64]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.nanoTimestamp()});

        const memory = Memtable.init(alloc, new_id) catch |err| {
            std.log.err("unable to init memtable {s}", .{@errorName(err)});
            return err;
        };
        errdefer {
            memory.deinit(alloc);
            alloc.destroy(memory);
        }

        const io = try alloc.create(SSTableStore);
        io.* = try SSTableStore.init(alloc, opts);
        errdefer alloc.destroy(io);

        const capacity = opts.sst_capacity;

        const db = alloc.create(Self) catch |err| {
            std.log.err("unable to allocate db {s}", .{@errorName(err)});
            return err;
        };

        // TODO: update allocator usage in WAL
        const wal = try allocator.create(WAL);
        wal.* = try WAL.init(allocator, .{ .Dir = opts.data_dir });

        db.* = .{
            .capacity = capacity,
            .opts = opts,
            .wal = wal,
            .memory = AtomicValue(*Memtable).init(memory),
            .io = io,
        };

        return db;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        var mtable = self.memory.load(.seq_cst);
        mtable.deinit(alloc);
        alloc.destroy(mtable);

        self.wal.deinit(allocator) catch unreachable;
        allocator.destroy(self.wal);

        self.io.deinit(alloc);
        alloc.destroy(self.io);

        self.* = undefined;
    }

    pub fn open(self: *Self, alloc: Allocator) !void {
        const data_dir = try std.fs.cwd().openDir(self.opts.data_dir, .{ .iterate = true });
        var dir_iter = data_dir.iterate();

        while (try dir_iter.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".mtab")) {
                var file_path_buf: [256]u8 = undefined;
                const file_path = try std.fmt.bufPrint(
                    &file_path_buf,
                    "{s}/{s}",
                    .{ self.opts.data_dir, entry.name },
                );

                const memtable = Memtable.deserializeFromDisk(alloc, file_path) catch |err| @panic(@errorName(err));

                self.memory.store(memtable, .seq_cst);

                std.log.info("loaded serialized memtable {s} from {s}", .{ memtable.getId(), file_path });

                break;
            }
        }

        // TODO: Open wal

        self.io.open(alloc) catch |err| {
            std.log.err("not able to open sstables data directory {s}", .{@errorName(err)});
            return err;
        };

        const cnt = self.memory.load(.seq_cst).index.count;

        std.log.info(
            "database opened @ {s} w/ {d} hot keys",
            .{ self.opts.data_dir, cnt },
        );
    }

    pub fn shutdown(self: *Self, alloc: Allocator) !void {
        const memory = self.memory.load(.seq_cst);
        if (!memory.flushed() and memory.size() > 0) {
            var file_path_buf: [256]u8 = undefined;
            const file_path = try std.fmt.bufPrint(
                &file_path_buf,
                "{s}/memtable_{s}.mtab",
                .{ self.opts.data_dir, memory.getId() },
            );

            memory.flush();
            try memory.serializeToDisk(alloc, file_path);

            try self.xflush(alloc, memory);

            std.log.info("hot memtable {s} successfully serialized to disk at {s}", .{ memory.getId(), file_path });
        }

        // TODO: snapsot wal
    }

    pub fn read(self: *Self, key: []const u8) !?KV {
        if (key.len == 0) return null;

        const memory = self.memory.load(.acquire);
        if (try memory.get(key)) |kv| {
            return kv;
        }

        if (try self.io.read(key)) |kv| {
            return kv;
        }

        return null;
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
        errdefer alloc.destroy(merger);

        merger.* = try MergeIterator(KV, keyvalue.compare).init(alloc);

        const memory = self.memory.load(.seq_cst);
        if (memory.size() > 0) {
            var hot_iter = try memory.iterator(alloc);
            errdefer hot_iter.deinit();

            try merger.add(hot_iter);
        }

        var cold_iter = try self.io.iterator(alloc);
        errdefer cold_iter.deinit();

        try merger.add(cold_iter);

        const wrapper = try alloc.create(MergeIteratorWrapper);
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
        if (kv.value.len == 0) {
            return error.InvalidValueData;
        }

        try self.wal.write(allocator, kv);

        const mtable = self.memory.load(.seq_cst);
        try mtable.put(kv);
    }

    pub fn memtableCount(self: *Self) usize {
        _ = self;
        return 1;
    }

    pub fn freeze(self: *Self, alloc: Allocator, mtable: *Memtable) !void {
        if (mtable.frozen()) return;

        mtable.freeze();

        var new_id_buf: [64]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.nanoTimestamp()});

        const nxt_table = try Memtable.init(alloc, new_id);

        self.memory.store(nxt_table, .seq_cst);
    }

    pub fn flush(self: *Self, alloc: Allocator) !void {
        const memory = self.memory.load(.acquire);
        if (memory.size() == 0) return error.NothingToFlush;

        if (!memory.flushed()) {
            memory.flush();
            try self.xflush(alloc, memory);
        }

        var new_id_buf: [64]u8 = undefined;
        const new_id = try std.fmt.bufPrint(
            &new_id_buf,
            "{d}",
            .{std.time.nanoTimestamp()},
        );

        const nxt_table = try Memtable.init(alloc, new_id);
        self.memory.store(nxt_table, .seq_cst);
    }

    inline fn xflush(self: *Self, alloc: Allocator, memory: *Memtable) !void {
        self.io.flush(alloc, memory) catch |err| {
            std.log.err(
                "sstables not able to flush mtable {s} {s}",
                .{ memory.getId(), @errorName(err) },
            );
            return error.FlushFailed;
        };
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
        const expected = try KV.initOwned(alloc, key, value);
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

    var items = try std.ArrayList(KV).initCapacity(alloc, 10);
    defer items.deinit(alloc);

    var scan_iter = try db.scan(alloc, "__key_b__", "__key_d__");
    defer scan_iter.deinit();

    while (scan_iter.next()) |nxt| {
        try items.append(alloc, nxt);
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

test "flush with no memtables" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // flush should return error when no memtables to flush
    try testing.expectError(error.NothingToFlush, db.flush(alloc));
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

    const memory = db.memory.load(.seq_cst);

    // freeze once
    try db.freeze(alloc, memory);

    // freeze again - should be no-op
    try db.freeze(alloc, memory);

    // should still work without error
    try testing.expect(memory.frozen());
}

test "shutdown persists hot memtable to disk" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    // write some data to hot memtable
    var kv1 = try KV.initOwned(alloc, "shutdown_key1", "shutdown_value1");
    defer kv1.deinit(alloc);
    try db.write(kv1);

    var kv2 = try KV.initOwned(alloc, "shutdown_key2", "shutdown_value2");
    defer kv2.deinit(alloc);
    try db.write(kv2);

    const memory = db.memory.load(.seq_cst);
    const initial_size = memory.size();
    try testing.expect(initial_size > 0);

    // shutdown should persist the hot memtable
    try db.shutdown(alloc);

    // verify hot table was frozen
    try testing.expect(memory.frozen());
}

test "shutdown with empty hot memtable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit(alloc);

    const memory = db.memory.load(.seq_cst);
    try testing.expectEqual(@as(usize, 0), memory.size());

    // shutdown with empty hot table should not error
    try db.shutdown(alloc);
}
