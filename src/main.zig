const std = @import("std");

const atomic = std.atomic;
const debug = std.debug;
const heap = std.heap;
const io = std.io;
const math = std.math;
const mem = std.mem;
const testing = std.testing;
const hasher = std.hash.Murmur2_64;

const Allocator = mem.Allocator;
const ArenaAllocator = heap.ArenaAllocator;
const AtomicValue = atomic.Value;
const Mutex = std.Thread.Mutex;
const Order = math.Order;

const csv = @cImport({
    @cInclude("csv.h");
});
pub const CsvOpen2 = csv.CsvOpen2;
pub const CsvClose = csv.CsvClose;
pub const ReadNextRow = csv.CsvReadNextRow;
pub const ReadNextCol = csv.CsvReadNextCol;

pub const ThreadSafeBumpAllocator = @import("byte_arena.zig").ThreadSafeBumpAllocator;

pub const KV = @import("kv.zig").KV;

const file = @import("file.zig");
pub const OpenFile = file.open;

const msgq = @import("msgqueue.zig");
pub const ProcessMessageQueue = msgq.ProcessMessageQueue;
pub const ThreadMessageQueue = msgq.Queue;

const mtbl = @import("memtable.zig");
pub const Memtable = mtbl.Memtable;

const opt = @import("opts.zig");
pub const Opts = opt.Opts;
pub const defaultOpts = opt.defaultOpts;
pub const withDataDirOpts = opt.withDataDirOpts;

const prof = @import("profile.zig");
pub const BeginProfile = prof.BeginProfile;
pub const EndProfile = prof.EndProfile;
pub const BlockProfiler = prof.BlockProfiler;

const SSTable = @import("sstable.zig").SSTable;
const TableMap = @import("tablemap.zig").TableMap;
const WAL = @import("wal.zig").WAL;

var mtx: Mutex = .{};

pub const Database = struct {
    alloc: Allocator,
    capacity: usize,
    mtable: AtomicValue(*Memtable),
    opts: Opts,
    mtables: std.ArrayList(*Memtable),

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        const mtable = try Memtable.init(alloc, 0, opts);
        const mtables = std.ArrayList(*Memtable).init(alloc);
        const capacity = opts.sst_capacity;

        const db = try alloc.create(Self);
        db.* = .{
            .alloc = alloc,
            .capacity = capacity,
            .mtable = AtomicValue(*Memtable).init(mtable),
            .mtables = mtables,
            .opts = opts,
        };
        return db;
    }

    pub fn deinit(self: *Self) void {
        {
            mtx.lock();
            defer mtx.unlock();
            for (self.mtables.items) |mtable| {
                mtable.deinit();
                self.alloc.destroy(mtable);
            }
            self.mtables.deinit();
        }

        var mtable = self.mtable.load(.seq_cst);
        mtable.deinit();
        self.alloc.destroy(mtable);

        self.* = undefined;
    }

    pub fn allocator(self: Self) Allocator {
        return self.alloc;
    }

    pub fn read(self: Self, key: []const u8) !KV {
        if (self.mtable.load(.seq_cst).get(key)) |value| {
            return value;
        }

        {
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
    }

    const MergeIterator = struct {
        alloc: Allocator,
        mtbls: std.ArrayList(*Memtable.Iterator),
        queue: std.PriorityQueue(KV, void, lessThan),
        v: ?KV,

        fn lessThan(context: void, a: KV, b: KV) Order {
            _ = context;
            return mem.order(u8, a.key, b.key);
        }

        pub fn init(dalloc: Allocator, db: *const Database) !MergeIterator {
            var iters = std.ArrayList(*Memtable.Iterator).init(dalloc);
            {
                mtx.lock();
                defer mtx.unlock();

                const mtables = try dalloc.alloc(*Memtable.Iterator, db.mtables.items.len);
                for (db.mtables.items, 0..) |mtable, i| {
                    mtables[i].* = try mtable.iterator();
                    try iters.append(mtables[i]);
                }
            }

            const iter = try dalloc.create(Memtable.Iterator);
            iter.* = try db.mtable.load(.seq_cst).iterator();
            try iters.append(iter);

            var queue = std.PriorityQueue(KV, void, lessThan).init(dalloc, {});
            try queue.ensureTotalCapacity(std.mem.page_size);

            return .{
                .alloc = dalloc,
                .mtbls = iters,
                .queue = queue,
                .v = null,
            };
        }

        pub fn deinit(mi: *MergeIterator) void {
            for (mi.mtbls.items) |item| {
                item.deinit();
                mi.alloc.destroy(item);
            }
            mi.mtbls.deinit();
            mi.queue.deinit();
            mi.* = undefined;
        }

        pub fn value(mi: MergeIterator) KV {
            return mi.v.?;
        }

        pub fn next(mi: *MergeIterator) !bool {
            for (mi.mtbls.items) |table_iter| {
                if (try table_iter.next()) {
                    const kv = table_iter.value();
                    mi.queue.add(kv) catch |err| {
                        debug.print(
                            "merge iter next failure: {s}\n",
                            .{@errorName(err)},
                        );
                        return false;
                    };
                }
            }

            if (mi.queue.count() == 0) {
                return false;
            }

            const nxt = mi.queue.remove();
            mi.*.v = nxt;
            return true;
        }
    };

    pub fn iterator(self: *Self) !MergeIterator {
        return try MergeIterator.init(self.alloc, self);
    }

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0 or !std.unicode.utf8ValidateSlice(key)) {
            return error.WriteError;
        }

        const item = KV.init(key, value);

        var mtable = self.mtable.load(.seq_cst);
        if ((mtable.size() + item.len()) >= self.capacity) {
            // debug.print("freeze and flush current size {d} to add {d} @ capacity {d}...\n", .{
            //     mtable.size(), item.len(), self.capacity,
            // });
            try self.freezeAndFlush(mtable);
        }

        try self.mtable.load(.seq_cst).put(item);
    }

    pub fn flush(self: *Self) !void {
        const mtable = self.mtable.load(.seq_cst);
        try self.freezeAndFlush(mtable);
    }

    inline fn freezeAndFlush(self: *Self, mtable: *Memtable) !void {
        {
            mtx.lock();
            defer mtx.unlock();

            if (mtable.frozen()) {
                return;
            }

            mtable.freeze();

            try self.mtables.append(mtable);

            const current_id = mtable.getId();
            const nxt_table = try Memtable.init(self.alloc, current_id + 1, self.opts);
            self.mtable.store(nxt_table, .seq_cst);

            try mtable.flush();
        }
    }
};

pub fn defaultDatabase(alloc: Allocator) !*Database {
    return try databaseFromOpts(alloc, defaultOpts());
}

pub fn databaseFromOpts(alloc: Allocator, opts: Opts) !*Database {
    return try Database.init(alloc, opts);
}

test "basic functionality" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try databaseFromOpts(alloc, withDataDirOpts(dir_name));
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

    const db = try databaseFromOpts(alloc, withDataDirOpts(dir_name));
    defer alloc.destroy(db);
    defer db.deinit();

    // given
    const kvs = [3]KV{
        KV.init("__key_c__", "__value_c__"),
        KV.init("__key_b__", "__value_b__"),
        KV.init("__key_a__", "__value_a__"),
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
    var iter = try db.iterator();
    defer iter.deinit();

    var count: usize = 0;
    while (iter.next() catch false) {
        count += 1;
    }
    try testing.expectEqual(kvs.len, count);
}
