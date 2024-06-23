const std = @import("std");

const csv = @import("csv_reader.zig");
const log = @import("wal.zig");
const file_utils = @import("file.zig");
const mtbl = @import("memtable.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tm = @import("tablemap.zig");

const io = std.io;
const testing = std.testing;
const hasher = std.hash.Murmur2_64;
const Allocator = std.mem.Allocator;
const CsvTokenizer = csv.CsvTokenizer;
const Memtable = mtbl.Memtable;
const MemtableOpts = mtbl.MemtableOpts;
const Opts = options.Opts;
const Order = std.math.Order;
const Row = sst.SSTableRow;
const SSTable = sst.SSTable;
const SSTableOpts = sst.SSTableOpts;
const TableMap = tm.TableMap;
const WAL = log.WAL;

pub const CSV = CsvTokenizer(std.fs.File.Reader);
pub const MessageQueue = @import("msgqueue.zig").ProcessMessageQueue;
pub const MemtableList = TableMap(*Memtable(u64, []const u8));
pub const SSTableList = std.ArrayList(*SSTable);

const KV = struct {
    k: u64,
    v: []const u8,
};

fn lessThan(context: void, a: KV, b: KV) Order {
    _ = context;
    return std.math.order(a.k, b.k);
}

const Database = struct {
    alloc: Allocator,
    capacity: usize,
    mtable: *Memtable(u64, []const u8),
    opts: Opts,
    mtables: *MemtableList,
    sstables: SSTableList,

    const Self = @This();
    const Error = error{};

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        const mtables = try MemtableList.init(alloc);
        const sstables = SSTableList.init(alloc);
        const mtable = try Memtable(u64, []const u8).init(alloc, 0, opts);
        const capacity = opts.sst_capacity / @sizeOf(Row);

        std.debug.print("init memtable cap {d} sstable opts {d} sizeof Row {d}\n", .{ capacity, opts.sst_capacity, @sizeOf(Row) });

        const db = try alloc.create(Self);
        db.* = .{
            .alloc = alloc,
            .capacity = capacity,
            .mtable = mtable,
            .opts = opts,
            .mtables = mtables,
            .sstables = sstables,
        };
        return db;
    }

    pub fn deinit(self: *Self) void {
        self.flush() catch |err| {
            std.debug.print("db flush error {s}\n", .{@errorName(err)});
        };
        self.mtable.deinit();
        self.alloc.destroy(self.mtable);
        var mtable_iter = self.mtables.iterator(0);
        while (mtable_iter.next()) {
            var table = mtable_iter.value();
            table.deinit();
            self.alloc.destroy(table);
        }
        self.mtables.deinit();
        self.alloc.destroy(self.mtables);
        for (self.sstables.items) |table| {
            table.deinit();
            self.alloc.destroy(table);
        }
        self.sstables.deinit();
        self.* = undefined;
    }

    fn lookup(self: Self, key: u64) ?[]const u8 {
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

        for (self.sstables.items) |table| {
            const value = table.read(key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return null,
            };
            return value;
        }
        return null;
    }

    pub fn read(self: Self, key: []const u8) ?[]const u8 {
        const k: u64 = hasher.hash(key);
        return self.lookup(k);
    }

    const MergeIterator = struct {
        mtbls: std.ArrayList(*Memtable(u64, []const u8).Iterator),
        queue: std.PriorityQueue(KV, void, lessThan),
        k: u64,
        v: []const u8,

        pub fn init(alloc: Allocator, m: *Database) !*MergeIterator {
            var iters = std.ArrayList(*Memtable(u64, []const u8).Iterator).init(alloc);
            var table_iter = m.mtables.iterator(0);
            while (table_iter.next()) {
                const iter = try table_iter.value().iterator();
                try iters.append(iter);
            }

            const queue = std.PriorityQueue(KV, void, lessThan).init(alloc, {});
            std.debug.print("MergeIterator init with {d} memtables & 1 priority queue\n", .{iters.items.len});

            const mi = try alloc.create(MergeIterator);
            mi.*.mtbls = iters;
            mi.*.queue = queue;
            return mi;
        }

        pub fn deinit(self: *MergeIterator) void {
            self.mtbls.deinit();
            self.queue.deinit();
            self.* = undefined;
        }

        pub fn key(mi: MergeIterator) u64 {
            return mi.k;
        }

        pub fn value(mi: MergeIterator) []const u8 {
            return mi.v;
        }

        pub fn next(mi: *MergeIterator) !bool {
            for (mi.mtbls.items) |table_iter| {
                if (table_iter.next()) {
                    const tkey = table_iter.key();
                    const tvalue = table_iter.value();
                    const row: KV = .{ .k = tkey, .v = tvalue };
                    try mi.queue.add(row);
                }
            }
            if (mi.queue.count() == 0) {
                return false;
            }
            const nxt = mi.queue.remove();
            mi.*.k = nxt.k;
            mi.*.v = nxt.v;
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
        if (self.mtable.count() >= self.capacity) {
            try self.flush();
        }
        const k: u64 = hasher.hash(key);
        try self.mtable.put(k, value);
    }

    fn freeze(self: *Self) !void {
        const current_id = self.mtable.getId();
        try self.mtables.put(current_id, self.mtable);
        self.mtable = try Memtable(u64, []const u8).init(self.alloc, current_id + 1, self.opts);
    }

    pub fn flush(self: *Self) !void {
        if (self.mtable.count() == 0) {
            return;
        }
        std.debug.print("flushing memtable...\n", .{});
        const current_id = self.mtable.getId();
        const filename = try std.fmt.allocPrint(self.alloc, "{s}/{d}.dat", .{ self.opts.data_dir, current_id });
        defer self.alloc.free(filename);
        const file = try file_utils.openWithCapacity(filename, self.opts.sst_capacity);
        const sstable = try SSTable.init(self.alloc, current_id, self.opts);
        try sstable.connect(file);
        try self.sstables.append(sstable);
        try self.mtable.flush(sstable);
        try self.freeze();
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
