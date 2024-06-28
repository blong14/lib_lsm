const std = @import("std");

const csv = @import("csv_reader.zig");
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
const CsvTokenizer = csv.CsvTokenizer;
const Memtable = mtbl.Memtable;
const MemtableOpts = mtbl.MemtableOpts;
const Opts = options.Opts;
const Order = std.math.Order;
const Row = sst.SSTableRow;
const SSTable = sst.SSTable;
const TableMap = tm.TableMap;
const WAL = log.WAL;

pub const CSV = CsvTokenizer(std.fs.File.Reader);
pub const MessageQueue = @import("msgqueue.zig").ProcessMessageQueue;
pub const SSTableList = std.ArrayList(*SSTable);

fn lessThan(context: void, a: KV, b: KV) Order {
    _ = context;
    return std.math.order(a.hash, b.hash);
}

const Database = struct {
    alloc: ArenaAllocator,
    capacity: usize,
    mtable: *Memtable(u64, KV),
    opts: Opts,
    mtables: *TableMap(*Memtable(u64, KV)),

    const Self = @This();
    const Error = error{};

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        var arena = ArenaAllocator.init(alloc);
        const mtable = try Memtable(u64, KV).init(alloc, 0, opts);
        const mtables = try TableMap(*Memtable(u64, KV)).init(alloc);
        const capacity = opts.sst_capacity / @sizeOf(Row);

        std.debug.print("init memtable cap {d} sstable opts {d} sizeof Row {d}\n", .{ capacity, opts.sst_capacity, @sizeOf(Row) });

        const allocator = arena.allocator();
        const db = try allocator.create(Self);
        db.* = .{
            .alloc = arena,
            .capacity = capacity,
            .mtable = mtable,
            .opts = opts,
            .mtables = mtables,
        };
        return db;
    }

    pub fn deinit(self: *Self) void {
        self.flush() catch |err| {
            std.debug.print("db flush error {s}\n", .{@errorName(err)});
        };
        self.mtable.deinit();
        var mtable_iter = self.mtables.iterator(0);
        while (mtable_iter.next()) {
            var table = mtable_iter.value();
            table.deinit();
        }
        self.mtables.deinit();
        self.alloc.deinit();
        self.* = undefined;
    }

    fn lookup(self: Self, key: u64) !KV {
        if (self.mtable.get(key)) |value| {
            return value;
        }

        var newest_to_oldest_iter = self.mtables.iterator(self.mtables.count() - 1);
        while (newest_to_oldest_iter.prev()) {
            const table = newest_to_oldest_iter.value();
            if (table.get(key)) |value| {
                return value;
            }
        }

        return error.NotFound;
    }

    pub fn read(self: Self, key: []const u8) !KV {
        const k: u64 = hasher.hash(key);
        return self.lookup(k);
    }

    const MergeIterator = struct {
        alloc: ArenaAllocator,
        mtbls: std.ArrayList(*Memtable(u64, KV).Iterator),
        queue: std.PriorityQueue(KV, void, lessThan),
        k: u64,
        v: KV,

        pub fn init(alloc: Allocator, m: *Database) !*MergeIterator {
            var arena = ArenaAllocator.init(alloc);
            var iters = std.ArrayList(*Memtable(u64, KV).Iterator).init(alloc);
            const queue = std.PriorityQueue(KV, void, lessThan).init(alloc, {});

            var table_iter = m.mtables.iterator(0);
            while (table_iter.next()) {
                const iter = try table_iter.value().iterator();
                try iters.append(iter);
            }

            std.debug.print("MergeIterator init with {d} memtables\n", .{iters.items.len});

            const mi = try arena.allocator().create(MergeIterator);
            mi.* = .{
                .alloc = arena,
                .mtbls = iters,
                .queue = queue,
                .k = 0,
                .v = undefined,
            };
            return mi;
        }

        pub fn deinit(self: *MergeIterator) void {
            self.mtbls.deinit();
            self.queue.deinit();
            self.alloc.deinit();
            self.* = undefined;
        }

        pub fn key(mi: MergeIterator) u64 {
            return mi.k;
        }

        pub fn value(mi: MergeIterator) KV {
            return mi.v;
        }

        pub fn next(mi: *MergeIterator) !bool {
            for (mi.mtbls.items) |table_iter| {
                if (table_iter.next()) {
                    const kv = table_iter.value();
                    try mi.queue.add(kv);
                }
            }
            if (mi.queue.count() == 0) {
                return false;
            }
            const nxt = mi.queue.remove();
            mi.*.k = nxt.hash;
            mi.*.v = nxt;
            return true;
        }
    };

    pub fn iterator(self: *Self) !*MergeIterator {
        return try MergeIterator.init(self.alloc.allocator(), self);
    }

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0) {
            return error.WriteError;
        }
        if (self.mtable.count() >= self.capacity) {
            try self.flush();
            try self.freeze();
        }
        const k: u64 = hasher.hash(key);

        const kv = try self.alloc.allocator().create(KV);
        kv.* = .{
            .hash = k,
            .key = key,
            .value = value,
        };
        try self.mtable.put(k, kv);
    }

    pub fn flush(self: *Self) !void {
        try self.mtable.flush();
    }

    fn freeze(self: *Self) !void {
        const current_id = self.mtable.getId();
        try self.mtables.put(current_id, self.mtable);
        self.mtable = try Memtable(u64, KV).init(self.alloc.allocator(), current_id + 1, self.opts);
    }
};

pub fn defaultDatabase(alloc: Allocator) !*Database {
    return try databaseFromOpts(alloc, options.defaultOpts());
}

pub fn databaseFromOpts(alloc: Allocator, opts: Opts) !*Database {
    return try Database.init(alloc, opts);
}

test "basic functionality" {
    var alloc = testing.allocator;

    var db = try defaultDatabase(alloc);
    defer alloc.destroy(db);
    defer db.deinit();

    // given
    const key = "__key__";
    const value = "__value__";

    // when
    try db.write(key, value);

    // then
    const actual = db.read(key);
    try testing.expectEqualStrings(value, actual.?);
}
