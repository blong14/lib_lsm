const std = @import("std");

const sst = @import("sstable.zig");
const CsvTokenizer = @import("csv_reader.zig").CsvTokenizer;
const mtbl = @import("memtable.zig");
const WAL = @import("wal.zig").WAL;
pub const MessageQueue = @import("msgqueue.zig").MessageQueue;
const options = @import("opts.zig");

const io = std.io;
const testing = std.testing;
const hasher = std.hash.Murmur2_64;
const Allocator = std.mem.Allocator;
const Memtable = mtbl.Memtable;
const MemtableOpts = mtbl.MemtableOpts;
const Opts = options.Opts;
const Row = sst.SSTableRow;
const SSTable = sst.SSTable;
const SSTableOpts = sst.SSTableOpts;

pub const CSV = CsvTokenizer(std.fs.File.Reader);
pub const MemtableList = std.ArrayList(*Memtable(u64, []const u8));
pub const SSTableList = std.ArrayList(*SSTable);

const Database = struct {
    alloc: Allocator,
    capacity: usize,
    mtable: *Memtable(u64, []const u8),
    opts: Opts,
    mtables: std.ArrayList(*Memtable(u64, []const u8)),
    sstables: std.ArrayList(*SSTable),

    const Self = @This();
    const Error = error{};

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        var mtables = MemtableList.init(alloc);
        var sstables = SSTableList.init(alloc);
        var mtable = try Memtable(u64, []const u8).init(alloc, opts);
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
        for (self.mtables.items) |table| {
            table.deinit();
            self.alloc.destroy(table);
        }
        self.mtables.deinit();
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
            for (self.mtables.items) |table| {
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
        }
        return null;
    }

    pub fn read(self: Self, key: []const u8) ?[]const u8 {
        const k: u64 = hasher.hash(key);
        return self.lookup(k);
    }

    pub fn scan(self: Self, start: []const u8, end: []const u8) ![]mtbl.Entry {
        const start_key = hasher.hash(start);
        const end_key = hasher.hash(end);
        var out = std.ArrayList(mtbl.Entry).init(self.alloc);
        try self.mtable.scan(start_key, end_key, &out);
        return out.toOwnedSlice();
    }

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0) return error.WriteError;
        const k: u64 = hasher.hash(key);
        if (self.mtable.count() >= self.capacity) {
            try self.flush();
        }
        try self.mtable.put(k, value);
    }

    fn flush(self: *Self) !void {
        std.debug.print("flushing...\n", .{});
        const pathname = self.opts.data_dir;
        const filename = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ pathname, "sstable.dat" });
        defer self.alloc.free(filename);
        var sstable = try SSTable.init(self.alloc, filename, self.opts.sst_capacity);
        try self.sstables.append(sstable);
        try self.mtable.flush(sstable);
        try self.mtables.append(self.mtable);
        self.mtable = try Memtable(u64, []const u8).init(self.alloc, self.opts);
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

test "scan" {
    var alloc = testing.allocator;

    var db = try defaultDatabase(alloc);
    defer alloc.destroy(db);
    defer db.deinit();

    // given
    const start = "akey_a";
    const end = "ckey_c";
    const value = "__value__";

    // when
    try db.write("akey_a", value);
    try db.write("bkey_b", value);
    try db.write("ckey_c", value);
    try db.write("dkey_d", value);

    // then
    const items = try db.scan(start, end);
    defer alloc.free(items);
    std.debug.print("{any}\n", .{items});
    try testing.expect(items.len == 3);
    // try testing.expectEqualStrings(value, actual.?);
}
