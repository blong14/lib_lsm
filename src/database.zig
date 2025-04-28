const std = @import("std");

const ba = @import("bump_allocator.zig");
const file = @import("file.zig");
const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const lsm = @import("lib.zig");
const mtbl = @import("memtable.zig");
const opt = @import("opts.zig");
const sst = @import("sstable.zig");
const tm = @import("tablemap.zig");

const atomic = std.atomic;
const debug = std.debug;
const math = std.math;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const AtomicValue = atomic.Value;
const Mutex = std.Thread.Mutex;
const Order = math.Order;

const ThreadSafeBumpAllocator = ba.ThreadSafeBumpAllocator;
const Iterator = iter.Iterator;
const MergeIterator = iter.MergeIterator;
const ScanIterator = iter.ScanIterator;
const KV = keyvalue.KV;
const Memtable = mtbl.Memtable;
const Opts = opt.Opts;
const SSTable = sst.SSTable;
const TableManager = sst.TableManager;

const TAG = "[zig]";
var mtx: Mutex = .{};

pub const Database = struct {
    alloc: Allocator,
    byte_allocator: ThreadSafeBumpAllocator,
    capacity: usize,
    mtable: AtomicValue(*Memtable),
    opts: Opts,
    mtables: std.ArrayList(*Memtable),
    sstables: *TableManager,

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        var byte_allocator = ThreadSafeBumpAllocator.init(alloc, 1096) catch |err| {
            std.log.err("{s} unable to init bump allocator {s}", .{ TAG, @errorName(err) });
            return err;
        };
        errdefer byte_allocator.deinit();

        var new_id_buf: [32]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.milliTimestamp()});

        const mtable = Memtable.init(alloc, new_id, opts) catch |err| {
            std.log.err("{s} unable to init memtable {s}", .{ TAG, @errorName(err) });
            return err;
        };
        errdefer mtable.deinit();

        const mtables = std.ArrayList(*Memtable).init(alloc);
        errdefer mtables.deinit();

        const sstables = try alloc.create(TableManager);
        sstables.* = try TableManager.init(alloc, opts);

        const capacity = opts.sst_capacity;

        const db = alloc.create(Self) catch |err| {
            std.log.err("{s} unable to allocate db {s}", .{ TAG, @errorName(err) });
            return err;
        };

        db.* = .{
            .alloc = alloc,
            .byte_allocator = byte_allocator,
            .capacity = capacity,
            .mtable = AtomicValue(*Memtable).init(mtable),
            .mtables = mtables,
            .opts = opts,
            .sstables = sstables,
        };
        return db;
    }

    pub fn deinit(self: *Self) void {
        mtx.lock();
        defer mtx.unlock();

        for (self.mtables.items) |mtable| {
            mtable.deinit();
            self.alloc.destroy(mtable);
        }
        self.mtables.deinit();

        var mtable = self.mtable.load(.seq_cst);
        mtable.deinit();
        self.alloc.destroy(mtable);

        self.sstables.deinit(self.alloc);
        self.alloc.destroy(self.sstables);

        self.byte_allocator.printStats();
        self.byte_allocator.deinit();

        self.* = undefined;
    }

    pub fn open(self: *Self) !void {
        std.log.info("{s} database opening...", .{TAG});

        self.sstables.open(self.alloc) catch |err| {
            std.log.err("{s} not able to open sstables data directory {s}\n", .{ TAG, @errorName(err) });
            return err;
        };

        for (self.sstables.get(0)) |sstable| {
            var siter = try sstable.iterator(self.alloc);
            defer siter.deinit();

            while (siter.next()) |nxt| {
                try self.write(nxt.key, nxt.value);
            }

            const mtable = self.mtable.load(.seq_cst);
            mtable.isFlushed.store(true, .seq_cst);
            try self.freeze(mtable);
        }

        std.log.info("{s} database opened @ {s} w/ {d} warm tables", .{ TAG, self.opts.data_dir, self.mtables.items.len });
    }

    pub fn read(self: Self, key: []const u8) !KV {
        if (self.mtable.load(.seq_cst).get(key)) |value| {
            return value;
        }

        mtx.lock();
        defer mtx.unlock();

        if (self.mtables.items.len > 0) {
            var idx: usize = self.mtables.items.len - 1;
            while (idx > 0) {
                var table = self.mtables.items[idx];
                if (table.get(key)) |value| {
                    return value;
                }
                idx -= 1;
            }
            return error.NotFound;
        } else {
            return error.NotFound;
        }
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
        const hot_iter = try hot_table.iterator(alloc);
        try merger.add(hot_iter);

        var warm_tables: std.ArrayList(*Memtable) = undefined;
        {
            mtx.lock();
            defer mtx.unlock();
            warm_tables = try self.mtables.clone();
        }
        defer warm_tables.deinit();

        for (warm_tables.items) |mtable| {
            const warm_iter = try mtable.iterator(alloc);
            try merger.add(warm_iter);
        }

        const siter = try self.sstables.iterator(alloc);
        try merger.add(siter);

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
        scanner: *ScanIterator(KV, keyvalue.compare),
        iterator: Iterator(KV),

        const Wrapper = @This();

        pub fn deinit(ctx: *anyopaque) void {
            const sw: *Wrapper = @ptrCast(@alignCast(ctx));
            sw.iterator.deinit();
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
        const start = KV.init(start_key, "");
        const end = KV.init(end_key, "");

        const base_iter = try self.iterator(alloc);

        const si = try alloc.create(ScanIterator(KV, keyvalue.compare));
        si.* = ScanIterator(KV, keyvalue.compare).init(base_iter, start, end);

        const wrapper = try alloc.create(ScanWrapper);
        errdefer alloc.destroy(wrapper);

        wrapper.* = .{
            .alloc = alloc,
            .iterator = si.iterator(),
            .scanner = si,
        };

        return Iterator(KV).init(wrapper, ScanWrapper.next, ScanWrapper.deinit);
    }

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0 or !std.unicode.utf8ValidateSlice(key)) {
            return error.WriteError;
        }

        const item = KV.init(key, value);

        var mtable = self.mtable.load(.seq_cst);
        if ((mtable.size() + item.len()) >= self.capacity) {
            mtx.lock();
            defer mtx.unlock();
            try self.freeze(mtable);
        }

        mtable = self.mtable.load(.seq_cst);
        try mtable.put(item);
    }

    pub fn flush(self: *Self) !void {
        mtx.lock();
        defer mtx.unlock();

        if (self.mtables.items.len == 0) {
            return error.NothingToFlush;
        }

        const hot_table = self.mtable.load(.seq_cst);
        try self.freeze(hot_table);

        for (self.mtables.items) |mtable| {
            if (!mtable.flushed()) {
                try self.sstables.flush(self.alloc, mtable);
            }
            mtable.deinit();
            self.alloc.destroy(mtable);
        }

        self.mtables.clearAndFree();
    }

    fn freeze(self: *Self, mtable: *Memtable) !void {
        if (mtable.frozen()) {
            return;
        }

        mtable.freeze();

        // Generate a new ID that is lexicographically greater than the current one
        var new_id_buf: [32]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.milliTimestamp()});

        const nxt_table = try Memtable.init(self.alloc, new_id, self.opts);

        self.mtables.append(mtable) catch |err| {
            debug.print("freeze {s}\n", .{@errorName(err)});
            return err;
        };

        self.mtable.store(nxt_table, .seq_cst);
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
    defer db.deinit();

    // given
    const key = "__key__";
    const value = "__value__";

    // when
    try db.write(key, value);

    // then
    const actual = try db.read(key);
    try testing.expectEqualStrings(value, actual.value);
}

test "basic functionality with many items" {
    const alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(dir_name);
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer alloc.destroy(db);
    defer db.deinit();

    // given
    const kvs = [5]KV{
        KV.init("__key_c__", "__value_c__"),
        KV.init("__key_b__", "__value_b__"),
        KV.init("__key_d__", "__value_d__"),
        KV.init("__key_a__", "__value_a__"),
        KV.init("__key_e__", "__value_e__"),
    };

    // when
    for (kvs) |kv| {
        try db.write(kv.key, kv.value);
    }

    // then
    for (kvs) |kv| {
        const actual = try db.read(kv.key);
        try testing.expectEqualStrings(kv.value, actual.value);
    }

    // then
    var it = try db.iterator(alloc);
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |_| {
        count += 1;
    }

    try testing.expectEqual(kvs.len, count);

    var items = std.ArrayList(KV).init(alloc);
    defer items.deinit();

    var scan_iter = try db.scan(alloc, "__key_b__", "__key_e__");
    defer scan_iter.deinit();

    while (scan_iter.next()) |nxt| {
        try items.append(nxt);
    }

    try testing.expectEqual(3, items.items.len);
}

test "Database thread safety" {
    const alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(dir_name);
    defer testDir.dir.deleteTree(dir_name) catch {};

    const Thread = std.Thread;

    // given
    var o = opt.withDataDirOpts(dir_name);
    o.sst_capacity = 100;

    const db = try lsm.databaseFromOpts(alloc, o);
    defer alloc.destroy(db);
    defer db.deinit();

    const thread_fn = struct {
        fn run(mt: *Database, id: usize) void {
            var i: usize = 0;
            while (i < 100) : (i += 1) {
                const key = std.fmt.allocPrint(alloc, "key_{d}_{d}", .{ id, i }) catch continue;
                defer alloc.free(key);

                const value = std.fmt.allocPrint(alloc, "value_{d}_{d}", .{ id, i }) catch continue;
                defer alloc.free(value);

                mt.write(key, value) catch continue;
            }
        }
    }.run;

    var threads = std.ArrayList(Thread).init(alloc);
    defer threads.deinit();

    for (0..4) |i| {
        const thread = Thread.spawn(.{}, thread_fn, .{ db, i }) catch @panic("Failed to spawn thread");
        try threads.append(thread);
    }

    for (threads.items) |thread| {
        thread.join();
    }

    var flush_cnt: AtomicValue(u8) = AtomicValue(u8).init(0);

    // Test concurrent flush attempts
    const flush_fn = struct {
        fn run(mt: *Database, cnt: *AtomicValue(u8)) void {
            mt.flush() catch return;
            _ = cnt.fetchAdd(1, .seq_cst);
        }
    }.run;

    threads.clearRetainingCapacity();

    try threads.append(Thread.spawn(.{}, flush_fn, .{ db, &flush_cnt }) catch @panic("Failed to spawn thread"));
    try threads.append(Thread.spawn(.{}, flush_fn, .{ db, &flush_cnt }) catch @panic("Failed to spawn thread"));
    try threads.append(Thread.spawn(.{}, flush_fn, .{ db, &flush_cnt }) catch @panic("Failed to spawn thread"));

    for (threads.items) |thread| {
        thread.join();
    }

    try testing.expectEqual(1, flush_cnt.load(.seq_cst));
}
