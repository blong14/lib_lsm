const std = @import("std");

const csv = @import("csv_reader.zig");
const fio = @import("file.zig");
const log = @import("wal.zig");
const mtbl = @import("memtable.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tm = @import("tablemap.zig");
const KV = @import("kv.zig").KV;

const io = std.io;
const testing = std.testing;
const hasher = std.hash.Murmur2_64;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const MemoryPool = std.heap.MemoryPool;
const CsvTokenizer = csv.CsvTokenizer;
const Memtable = mtbl.Memtable;
const Order = std.math.Order;
const SSTable = sst.SSTable;
const TableMap = tm.TableMap;
const WAL = log.WAL;

pub const CSV = CsvTokenizer(std.fs.File.Reader);
pub const MessageQueue = @import("msgqueue.zig").ProcessMessageQueue;
pub const Opts = options.Opts;
pub const defaultOpts = options.defaultOpts;
pub const withDataDirOpts = options.withDataDirOpts;

const Profiler = @import("profile.zig");
pub const BeginProfile = Profiler.BeginProfile;
pub const EndProfile = Profiler.EndProfile;
pub const BlockProfiler = Profiler.BlockProfiler;

fn lessThan(context: void, a: KV, b: KV) Order {
    _ = context;
    return std.mem.order(u8, a.key, b.key);
}

pub const Database = struct {
    alloc: Allocator,
    capacity: usize,
    kv_pool: MemoryPool(KV),
    mtable: *Memtable,
    opts: Opts,
    mtables: std.ArrayList(*Memtable),

    const Self = @This();
    const Error = error{};

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        const mtable = try Memtable.init(alloc, 0, opts);
        const mtables = std.ArrayList(*Memtable).init(alloc);
        const capacity = opts.sst_capacity;
        const pool = try MemoryPool(KV).initPreheated(alloc, opts.sst_capacity);

        const db = try alloc.create(Self);
        db.* = .{
            .alloc = alloc,
            .kv_pool = pool,
            .capacity = capacity,
            .mtable = mtable,
            .mtables = mtables,
            .opts = opts,
        };
        return db;
    }

    pub fn deinit(self: *Self) void {
        self.flush() catch |err| {
            std.debug.print(
                "db flush error {s}\n",
                .{@errorName(err)},
            );
        };
        for (self.mtables.items) |mtable| {
            mtable.deinit();
            self.alloc.destroy(mtable);
        }
        self.mtable.deinit();
        self.alloc.destroy(self.mtable);
        self.mtables.deinit();
        self.kv_pool.deinit();
        self.* = undefined;
    }

    fn lookup(self: Self, key: []const u8) !KV {
        if (self.mtable.get(key)) |value| {
            return value;
        }
        var idx: usize = self.mtables.items.len - 1;
        while (idx > 0) {
            var table = self.mtables.items[idx];
            if (table.get(key)) |value| {
                return value;
            }
            idx -= 1;
        }
        return error.NotFound;
    }

    pub fn read(self: Self, key: []const u8) !KV {
        return self.lookup(key);
    }

    const MergeIterator = struct {
        alloc: Allocator,
        mtbls: std.ArrayList(*Memtable.Iterator),
        queue: std.PriorityQueue(KV, void, lessThan),
        v: KV,

        pub fn init(alloc: Allocator, m: *Database) !*MergeIterator {
            const queue = std.PriorityQueue(KV, void, lessThan).init(alloc, {});

            var iters = std.ArrayList(*Memtable.Iterator).init(alloc);
            for (m.mtables.items) |mtable| {
                const iter = try mtable.iterator(0);
                try iters.append(iter);
            }

            const iter = try m.mtable.iterator(0);
            try iters.append(iter);

            const mi = try alloc.create(MergeIterator);
            mi.* = .{
                .alloc = alloc,
                .mtbls = iters,
                .queue = queue,
                .v = undefined,
            };
            return mi;
        }

        pub fn deinit(mi: *MergeIterator) void {
            for (mi.mtbls.items) |iter| {
                mi.alloc.destroy(iter);
            }
            mi.mtbls.deinit();
            mi.queue.deinit();
            mi.* = undefined;
        }

        pub fn value(mi: MergeIterator) KV {
            return mi.v;
        }

        pub fn next(mi: *MergeIterator) !bool {
            for (mi.mtbls.items) |table_iter| {
                if (table_iter.next()) {
                    const kv = table_iter.value();
                    mi.queue.add(kv) catch |err| {
                        std.debug.print(
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

    pub fn iterator(self: *Self) !*MergeIterator {
        return try MergeIterator.init(self.alloc, self);
    }

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0) {
            return error.WriteError;
        }

        const item = try self.kv_pool.create();
        defer self.kv_pool.destroy(item);

        item.*.key = key;
        item.*.value = value;

        if ((self.mtable.size() + item.len()) >= self.capacity) {
            try self.flush();
            try self.freeze();
        }

        try self.mtable.put(item);
    }

    pub fn flush(self: *Self) !void {
        try self.mtable.flush();
    }

    inline fn freeze(self: *Self) !void {
        const current_id = self.mtable.getId();
        try self.mtables.append(self.mtable);
        self.mtable = try Memtable.init(self.alloc, current_id + 1, self.opts);
    }
};

pub fn defaultDatabase(alloc: Allocator) !*Database {
    return try databaseFromOpts(alloc, options.defaultOpts());
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
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try databaseFromOpts(alloc, withDataDirOpts(dir_name));
    defer db.deinit();

    // given
    var kvs: [3]*KV = undefined;
    kvs[0] = try KV.init(alloc, "__key_c__", "__value_c__");
    kvs[1] = try KV.init(alloc, "__key_b__", "__value_b__");
    kvs[2] = try KV.init(alloc, "__key_a__", "__value_a__");

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
