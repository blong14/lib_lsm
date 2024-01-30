const std = @import("std");

const sst = @import("sstable.zig");
const Memtable = @import("memtable.zig").Memtable;
const WAL = @import("wal.zig").WAL;

const io = std.io;
const testing = std.testing;
const hasher = std.hash.Murmur2_64;
const Allocator = std.mem.Allocator;
const SSTable = sst.SSTable;
const Row = sst.SSTableRow;

const DatabaseOpts = struct {
    data_dir: []const u8,
    // write-ahead log opts
    wal_capacity: usize,
    // sstable opts
    sst_capacity: usize,
};

const Database = struct {
    alloc: Allocator,
    mtable: Memtable(u64, []const u8),
    opts: DatabaseOpts,
    sstables: std.ArrayList(*SSTable),
    wal: WAL,

    const Self = @This();
    const Error = error{};

    pub fn init(alloc: Allocator, opts: DatabaseOpts) !Self {
        var wal = try WAL.init(opts.data_dir, opts.wal_capacity);
        var sstables = std.ArrayList(*SSTable).init(alloc);
        var memtable = try Memtable(u64, []const u8).init(alloc);
        return .{
            .alloc = alloc,
            .mtable = memtable,
            .opts = opts,
            .sstables = sstables,
            .wal = wal,
        };
    }

    pub fn deinit(self: *Self) void {
        self.mtable.deinit();
        for (self.sstables.items) |table| {
            table.deinit();
        }
        self.* = undefined;
    }

    fn maybeContains(self: Self, key: u64) bool {
        _ = self;
        _ = key;
        return true;
    }

    fn lookup(self: Self, key: u64) ?[]const u8 {
        if (self.maybeContains(key)) {
            if (self.mtable.get(key)) |value| {
                return value;
            }
            for (self.sstables.items) |table| {
                const value = table.read(key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return null,
                };
                return value;
            }
        }
        return null;
    }

    pub fn read(self: Self, key: []const u8) ?[]const u8 {
        const k: u64 = hasher.hash(key);
        return self.lookup(k);
    }

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        const k: u64 = hasher.hash(key);
        try self.wal.write(k, value);
        try self.mtable.put(k, value);
    }

    fn flush(self: *Self) !void {
        var sstable = try SSTable(Row).init(
            self.alloc, self.opts.data_dir, self.opts.sst_capacity);
        self.sstables.append(sstable);
        try self.mtable.flush(sstable);
        var tmp = self.mtable;
        defer tmp.deinit();
        self.mtable = try Memtable(i64, []const u8).init(self.alloc);
    }
};

pub fn defaultDatabase(alloc: Allocator) !Database {
    const opts: DatabaseOpts = .{
        .data_dir = "data",
        .wal_capacity = std.mem.page_size,
        .sst_capacity = std.mem.page_size,
    };
    return try databaseFromOpts(alloc, opts);
}

pub fn databaseFromOpts(alloc: Allocator, opts: DatabaseOpts) !Database {
    return try Database.init(alloc, opts);
}

test "basic functionality" {
    var alloc = testing.allocator;

    var db = try defaultDatabase(alloc);
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
