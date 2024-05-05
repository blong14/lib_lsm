const std = @import("std");

const MMap = @import("mmap.zig").MMap;
const TableMap = @import("tablemap.zig").TableMap;
const options = @import("opts.zig");

const Allocator = std.mem.Allocator;
const Opts = options.Opts;

pub const SSTableRow = extern struct {
    key: u64,
    value: [*c]const u8,
};

pub const SSTable = struct {
    alloc: Allocator,
    capacity: usize,
    connected: bool,
    data: *MMap(SSTableRow),
    file: std.fs.File,
    id: u64,
    index: *TableMap(IndexValue),
    mutable: bool,

    const Self = @This();

    const Error = error{
        NotFound,
    } || MMap(SSTableRow).Error;

    const IndexValue = struct {
        idx: usize,
        key: u64,
    };

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity / @sizeOf(SSTableRow);
        const index = try TableMap(IndexValue).init(alloc);
        const data = try MMap(SSTableRow).init(alloc, opts.sst_capacity);
        const st = try alloc.create(Self);
        st.* = .{
            .alloc = alloc,
            .capacity = cap,
            .connected = false,
            .data = data,
            .file = undefined,
            .id = id,
            .index = index,
            .mutable = false,
        };
        return st;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.alloc.destroy(self.data);
        self.index.deinit();
        self.alloc.destroy(self.index);
        if (self.connected) {
            self.file.close();
        }
        self.* = undefined;
    }

    pub fn connect(self: *Self, file: std.fs.File) !void {
        try self.data.connect(file);
        self.file = file;
        self.connected = true;
        self.mutable = true;
    }

    pub fn read(self: *Self, key: u64) ![]const u8 {
        if (!self.connected) {
            return error.NotConnected;
        }
        const count = self.data.getCount();
        if (count == 0) {
            return Error.NotFound;
        }
        const data = try self.index.get(key);
        const row = try self.data.read(data.idx);
        return std.mem.span(row.value);
    }

    pub fn write(self: *Self, key: u64, value: []const u8) !void {
        if (self.mutable and self.connected) {
            try self.data.append(SSTableRow{ .key = key, .value = value.ptr });
            try self.index.put(key, IndexValue{
                .idx = self.data.getCount() - 1,
                .key = key,
            });
            return;
        }
        return Error.WriteError;
    }
};

test "SSTable" {
    const FileUtils = @import("file.zig");

    const testing = std.testing;
    var alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "sstable.dat" });
    defer alloc.free(filename);
    const dopts = options.defaultOpts();
    const file = try FileUtils.openWithCapacity(filename, dopts.sst_capacity);

    // given
    var st = try SSTable.init(alloc, 1, dopts);
    defer alloc.destroy(st);
    defer st.deinit();
    try st.connect(file);

    // when
    var key: u64 = std.hash.Murmur2_64.hash("__key__");
    var expected: []const u8 = "__value__";

    try st.write(key, expected);
    var actual = try st.read(key);

    // then
    try testing.expect(std.mem.eql(u8, expected, actual));

    // when
    key = std.hash.Murmur2_64.hash("__key_a__");
    expected = "__value_a__";

    try st.write(key, expected);
    actual = try st.read(key);

    // then
    try testing.expect(std.mem.eql(u8, expected, actual));
}
