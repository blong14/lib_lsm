const std = @import("std");

const Allocator = std.mem.Allocator;
const File = std.fs.File;
const FixedBuffer = std.io.FixedBufferStream;

const copyForwards = std.mem.copyForwards;

pub fn MMap(comptime T: type) type {
    return struct {
        buf: FixedBuffer([]align(std.mem.page_size) u8),
        count: usize,
        file: File,
        len: usize,
        size: usize,

        const Self = @This();

        pub const Error = error{
            EndOfStream,
            ReadError,
            PipeBusy,
            SharingViolation,
            WriteError,
        };

        pub fn init(alloc: Allocator, file: std.fs.File, capacity: usize) !*Self {
            const data = try std.posix.mmap(
                null,
                capacity,
                std.posix.PROT.READ | std.posix.PROT.WRITE,
                .{ .TYPE = .SHARED, .ANONYMOUS = false },
                file.handle,
                0,
            );
            const mmap = try alloc.create(Self);
            mmap.* = .{
                .buf = std.io.fixedBufferStream(data),
                .count = 0,
                .file = file,
                .len = capacity / @sizeOf(T),
                .size = @sizeOf(T),
            };
            return mmap;
        }

        pub fn deinit(self: *Self) void {
            std.posix.munmap(self.buf.buffer);
            self.* = undefined;
        }

        pub fn append(self: *Self, value: T) !void {
            if (self.count == self.len) {
                return Error.WriteError;
            }
            try self.buf.seekTo(self.size * self.count);
            try self.buf.writer().writeStruct(value);
            self.count += 1;
        }

        pub fn insert(self: *Self, idx: usize, value: T) !void {
            if (self.count == self.len) {
                return Error.WriteError;
            }
            const index: usize = self.size * idx;
            try self.buf.seekTo(index);
            const start: usize = try self.buf.getPos();
            const end: usize = try self.buf.getEndPos() - self.size;
            copyForwards(u8, self.buf.buffer[start + self.size ..], self.buf.buffer[start..end]);
            try self.buf.writer().writeStruct(value);
            self.count += 1;
        }

        pub fn update(self: *Self, idx: usize, value: T) !void {
            const index: usize = self.size * idx;
            if (index >= self.len) {
                return Error.WriteError;
            }
            try self.buf.seekTo(index);
            try self.buf.writer().writeStruct(value);
        }

        pub fn read(self: *Self, idx: usize) !T {
            const index: usize = self.size * idx;
            if (index >= self.len) {
                return Error.ReadError;
            }
            try self.buf.seekTo(index);
            return try self.buf.reader().readStruct(T);
        }

        pub fn getCount(self: Self) usize {
            return self.count;
        }
    };
}

test "MMap append" {
    const XFile = @import("file.zig");
    const testing = std.testing;
    var alloc = testing.allocator;

    const Row = extern struct {
        key: u64,
        value: [*c]const u8,
    };

    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    // given
    const key = std.hash.Murmur2_64.hash("__key__");
    const expected: Row = .{ .key = key, .value = "__value__" };

    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "append.dat" });
    defer alloc.free(filename);

    var file = try XFile.openWithCapacity(filename, std.mem.page_size);
    defer file.close();

    var map = try MMap(Row).init(alloc, file, std.mem.page_size);
    defer alloc.destroy(map);
    defer map.deinit();

    // when
    try map.append(Row{ .key = std.hash.Murmur2_64.hash("__key_0__"), .value = "__value0__" });
    try map.append(Row{ .key = std.hash.Murmur2_64.hash("__key_1__"), .value = "__value1__" });
    try map.append(Row{ .key = std.hash.Murmur2_64.hash("__key_2__"), .value = "__value2__" });
    try map.append(expected);

    // then
    try testing.expect(map.count == 4);

    const actual = try map.read(3);
    try testing.expect(expected.key == actual.key);
}

test "MMap insert" {
    const XFile = @import("file.zig");
    const testing = std.testing;
    var alloc = testing.allocator;

    const Row = extern struct {
        key: u64,
        value: [*c]const u8,
    };

    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    // given
    const key = std.hash.Murmur2_64.hash("__key__");
    const expected: Row = .{ .key = key, .value = "__expected__" };

    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "insert.dat" });
    defer alloc.free(filename);

    var file = try XFile.openWithCapacity(filename, std.mem.page_size);
    defer file.close();

    var map = try MMap(Row).init(alloc, file, std.mem.page_size);
    defer alloc.destroy(map);
    defer map.deinit();

    // when
    try map.insert(0, Row{
        .key = std.hash.Murmur2_64.hash("__key_0__"),
        .value = "__value0__",
    });
    try map.insert(1, Row{
        .key = std.hash.Murmur2_64.hash("__key_1__"),
        .value = "__value1__",
    });
    try map.insert(2, Row{
        .key = std.hash.Murmur2_64.hash("__key_2__"),
        .value = "__value2__",
    });
    try map.insert(1, expected);

    // then
    try testing.expect(map.count == 4);

    const actual = try map.read(1);
    try testing.expect(expected.key == actual.key);
}
