const std = @import("std");

const csv = @import("csv_reader.zig");
const log = @import("wal.zig");
const mtbl = @import("memtable.zig");
const sst = @import("sstable.zig");
const tm = @import("tablemap.zig");
const options = @import("opts.zig");

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
pub const MessageQueue = @import("msgqueue.zig").MessageQueue;
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
        var mtables = try MemtableList.init(alloc);
        var sstables = SSTableList.init(alloc);
        var mtable = try Memtable(u64, []const u8).init(alloc, 0, opts);
        var capacity = opts.sst_capacity / @sizeOf(Row);

        std.debug.print("init memtable cap {d} sstable opts {d} sizeof Row {d}\n", .{ capacity, opts.sst_capacity, @sizeOf(Row) });

        var db = try alloc.create(Self);
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
        mtbls: std.ArrayList(Memtable(u64, []const u8).Iterator),
        queue: std.PriorityQueue(KV, void, lessThan),
        k: u64,
        v: []const u8,

        pub fn init(alloc: Allocator, m: *MemtableList) MergeIterator {
            var iters = std.ArrayList(Memtable(u64, []const u8).Iterator).init(alloc);
            var table_iter = m.mtbls.iterator(0);
            while (table_iter.next()) {
                var iter = table_iter.value().Iterator();
                try iters.append(iter);
            }
            return .{
                .mtbls = iters,
            };
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
            const nxt = mi.queue.remove();
            mi.*.k = nxt.k;
            mi.*.v = nxt.v;
        }
    };

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0) return error.WriteError;
        const k: u64 = hasher.hash(key);
        if (self.mtable.count() >= self.capacity) {
            try self.freeze();
        }
        try self.mtable.put(k, value);
    }

    fn freeze(self: *Self) !void {
        const current_id = self.mtable.getId();
        try self.mtables.put(current_id, self.mtable);
        self.mtable = try Memtable(u64, []const u8).init(self.alloc, current_id + 1, self.opts);
    }

    fn flush(self: *Self) !void {
        std.debug.print("flushing...\n", .{});
        const pathname = self.opts.data_dir;
        const filename = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ pathname, "sstable.dat" });
        defer self.alloc.free(filename);
        var sstable = try SSTable.init(self.alloc, filename, self.opts.sst_capacity);
        try self.sstables.append(sstable);
        try self.mtable.flush(sstable);
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
