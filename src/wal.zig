const std = @import("std");

const MMap = @import("mmap.zig").MMap;

const Allocator = std.mem.Allocator;

pub const WAL = struct {
    const Row = extern struct {
        key: u64,
        value: [*c]const u8,
    };

    data: MMap(Row),

    const Self = @This();

    const Error = error{
        NotFound,
    } || MMap(Row).Error;

    pub fn init(path: []const u8, capacity: usize) Error!Self {
        var data = try MMap(Row).init(path, capacity);
        return .{.data = data};
    }

    pub fn write(self: *Self, key: u64, value: []const u8) Error!void {
        return try self.data.append(Row{ .key = key, .value = value.ptr });
    }

    pub const Iterator = struct {
        data: std.io.Reader,

        pub fn next(it: *Iterator) ?Row {
            if (it.data.read()) |row| {
                return row;
            }
            return null;
        }

        pub fn reset(it: *Iterator) void {
            it.hash_iter.reset();
        }
    };

    pub fn iterator(self: Self) Iterator {
        return .{.data = self.data.buf.reader()};
    }
};

test WAL {
    const testing = std.testing;

    var alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteDir(pathname) catch {};

    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{pathname, "sstable.dat"});
    defer alloc.free(filename);
    var st = try WAL.init(filename, std.mem.page_size);

    const key = std.hash.Murmur2_64.hash("__key__");
    const expected = "__value__";
    try st.write(key, expected);
}