const std = @import("std");

const File = @import("file.zig");
const MMap = @import("mmap.zig").MMap;

const Allocator = std.mem.Allocator;

pub const WAL = struct {
    alloc: Allocator,
    data: *MMap(Row),
    file: std.fs.File,

    const Row = extern struct {
        key: u64,
        value: [*c]const u8,
    };

    const Self = @This();

    const Error = error{
        NotFound,
    } || MMap(Row).Error;

    pub fn init(alloc: Allocator, path: []const u8, capacity: usize) !*Self {
        const file = try File.openWithCapacity(path, capacity);
        const data = try MMap(Row).init(alloc, file, capacity);
        const wal = try alloc.create(Self);
        wal.* = .{ .alloc = alloc, .data = data, .file = file };
        return wal;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.alloc.destroy(self.data);
        self.file.close();
        self.* = undefined;
    }

    pub fn write(self: *Self, key: u64, value: []const u8) !void {
        return try self.data.append(Row{ .key = key, .value = value.ptr });
    }

    pub const Result = struct {
        key: u64,
        value: []const u8,
    };

    pub const Iterator = struct {
        idx: usize,
        data: *MMap(Row),

        pub fn next(it: *Iterator) ?Result {
            const row = it.data.read(it.*.idx) catch return null;
            it.*.idx += 1;
            const value = std.mem.span(row.value);
            return .{
                .key = row.key,
                .value = value,
            };
        }

        pub fn reset(it: *Iterator) void {
            it.*.idx = 0;
        }
    };

    pub fn iterator(self: Self) Iterator {
        return .{ .idx = 0, .data = self.data };
    }
};

test WAL {
    const testing = std.testing;
    var alloc = testing.allocator;

    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    // given
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "wal.dat" });
    defer alloc.free(filename);

    var st = try WAL.init(alloc, filename, std.mem.page_size);
    defer alloc.destroy(st);
    defer st.deinit();

    // when
    const key: u64 = std.hash.Murmur2_64.hash("__key__");
    const expected: []const u8 = "__value__";
    try st.write(key, expected);

    // then
    var iter = st.iterator();
    const actual = iter.next();
    try testing.expectEqualStrings(expected, actual.?.value);
}
