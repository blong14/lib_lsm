const std = @import("std");

const sst = @import("sstable.zig");
const CsvTokenizer = @import("csv_reader.zig").CsvTokenizer;
const Memtable = @import("memtable.zig").Memtable;
const WAL = @import("wal.zig").WAL;

const io = std.io;
const testing = std.testing;
const hasher = std.hash.Murmur2_64;
const Allocator = std.mem.Allocator;
const SSTable = sst.SSTable;
const Row = sst.SSTableRow;

pub const CSV = CsvTokenizer(std.fs.File.Reader);

const DatabaseOpts = struct {
    data_dir: []const u8,
    // write-ahead log opts
    wal_capacity: usize,
    // sstable opts
    sst_capacity: usize,
};

const Database = struct {
    alloc: Allocator,
    capacity: usize,
    mtable: Memtable(u64, []const u8),
    opts: DatabaseOpts,
    sstables: std.ArrayList(*SSTable),
    wal: WAL,

    const Self = @This();
    const Error = error{};

    pub fn init(alloc: Allocator, opts: DatabaseOpts) !Self {
        const pathname = opts.data_dir;
        const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{pathname, "wal.dat"});
        defer alloc.free(filename);

        var wal = try WAL.init(filename, opts.wal_capacity);
        var sstables = std.ArrayList(*SSTable).init(alloc);
        var memtable = try Memtable(u64, []const u8).init(alloc);
        var capacity = opts.sst_capacity / @sizeOf(Row);

        return .{
            .alloc = alloc,
            .capacity = capacity,
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
            self.alloc.destroy(table);
        }
        self.sstables.deinit();
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
        if (self.mtable.count() >= self.capacity) {
            try self.flush();
        }
        try self.mtable.put(k, value);
    }

    fn flush(self: *Self) !void {
        const pathname = self.opts.data_dir;
        const filename = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{pathname, "sstable.dat"});
        defer self.alloc.free(filename);
        var sstable = try SSTable.init(
            self.alloc, filename, self.opts.sst_capacity);
        try self.mtable.flush(sstable);
        try self.sstables.append(sstable);
        var tmp = self.mtable;
        defer tmp.deinit();
        self.mtable = try Memtable(u64, []const u8).init(self.alloc);
    }
};

pub fn defaultDatabase(alloc: Allocator) !Database {
    const opts: DatabaseOpts = .{
        .data_dir = "data",
        .wal_capacity = std.mem.page_size * std.mem.page_size,
        .sst_capacity = std.mem.page_size * std.mem.page_size,
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
