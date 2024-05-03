const std = @import("std");

const Allocator = std.mem.Allocator;
const File = std.fs.File;
const FixedBuffer = std.io.FixedBufferStream;

const copyForwards = std.mem.copyForwards;

pub fn MMap(comptime T: type) type {
    return struct {
        buf: FixedBuffer([]align(std.mem.page_size) u8),
        connected: bool,
        count: usize,
        file: File,
        len: usize,
        size: usize,

        const Self = @This();

        pub const Error = error{
            EndOfStream,
            NotConnected,
            ReadError,
            PipeBusy,
            SharingViolation,
            WriteError,
        };

        pub fn init(alloc: Allocator, capacity: usize) !*Self {
            const mmap = try alloc.create(Self);
            mmap.* = .{
                .buf = undefined,
                .connected = false,
                .count = 0,
                .file = undefined,
                .len = capacity / @sizeOf(T),
                .size = @sizeOf(T),
            };
            return mmap;
        }

        pub fn connect(self: *Self, file: std.fs.File) !void {
            const data = try std.posix.mmap(
                null,
                self.len,
                std.posix.PROT.READ | std.posix.PROT.WRITE,
                .{ .TYPE = .SHARED, .ANONYMOUS = false },
                file.handle,
                0,
            );
            self.buf = std.io.fixedBufferStream(data);
            self.file = file;
            self.connected = true;
        }

        pub fn deinit(self: *Self) void {
            if (self.connected) {
                std.posix.munmap(self.buf.buffer);
            }
            self.* = undefined;
        }

        pub fn append(self: *Self, value: T) !void {
            if (self.connected) {
                if (self.count == self.len) {
                    return Error.WriteError;
                }
                try self.buf.seekTo(self.size * self.count);
                try self.buf.writer().writeStruct(value);
                self.count += 1;
                return;
            }
            return Error.NotConnected;
        }

        pub fn insert(self: *Self, idx: usize, value: T) !void {
            if (self.connected) {
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
                return;
            }
            return Error.NotConnected;
        }

        pub fn update(self: *Self, idx: usize, value: T) !void {
            if (self.connected) {
                const index: usize = self.size * idx;
                if (index >= self.len) {
                    return Error.WriteError;
                }
                try self.buf.seekTo(index);
                try self.buf.writer().writeStruct(value);
                return;
            }
            return Error.NotConnected;
        }

        pub fn read(self: *Self, idx: usize) !T {
            if (self.connected) {
                const index: usize = self.size * idx;
                if (index >= self.len) {
                    return Error.ReadError;
                }
                try self.buf.seekTo(index);
                return try self.buf.reader().readStruct(T);
            }
            return Error.NotConnected;
        }

        pub fn getCount(self: Self) usize {
            return self.count;
        }
    };
}

const AppendOnlyMMap = struct {
    buf: FixedBuffer([]align(std.mem.page_size) u8),
    connected: bool,
    count: usize,
    file: File,
    last_idx: usize,
    len: usize,

    const Self = @This();

    pub const Error = error{
        NotConnected,
        EndOfStream,
        ReadError,
        PipeBusy,
        SharingViolation,
        WriteError,
    };

    pub const Result = struct {
        idx: usize,
        len: usize,
    };

    pub fn init(alloc: Allocator, capacity: usize) !*Self {
        const mmap = try alloc.create(Self);
        mmap.* = .{
            .buf = undefined,
            .connected = false,
            .count = 0,
            .file = undefined,
            .last_idx = 0,
            .len = capacity,
        };
        return mmap;
    }

    pub fn connect(self: *Self, file: std.fs.File) !void {
        const data = try std.posix.mmap(
            null,
            self.len,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED, .ANONYMOUS = false },
            file.handle,
            0,
        );
        self.buf = std.io.fixedBufferStream(data);
        self.file = file;
        self.connected = true;
    }

    pub fn deinit(self: *Self) void {
        if (self.connected) {
            std.posix.munmap(self.buf.buffer);
        }
        self.* = undefined;
    }

    pub fn read(self: *Self, idx: usize, value: []u8) !Result {
        if (self.connected) {
            try self.buf.seekTo(idx);
            const len = try self.buf.reader().read(value);
            return .{
                .idx = idx,
                .len = len,
            };
        }
        return Error.NotConnected;
    }

    pub fn write(self: *Self, value: []const u8) !Result {
        if (self.connected) {
            const current_idx = self.last_idx;
            try self.buf.seekTo(current_idx);
            const len = try self.buf.writer().write(value);
            self.last_idx += value.len;
            self.count += 1;
            return .{
                .idx = current_idx,
                .len = len,
            };
        }
        return Error.NotConnected;
    }

    pub fn getCount(self: Self) usize {
        return self.count;
    }
};

test "AppendOnlyMMap write" {
    const FileUtils = @import("file.zig");

    const testing = std.testing;
    var alloc = testing.allocator;
    const testdir = testing.tmpDir(.{});

    const pathname = try testdir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testdir.dir.deleteDir(pathname) catch {};
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "write.dat" });
    defer alloc.free(filename);
    var file = try FileUtils.openWithCapacity(filename, std.mem.page_size);
    defer file.close();

    // given
    const expected = "__value__";

    var map = try AppendOnlyMMap.init(alloc, std.mem.page_size);
    defer alloc.destroy(map);
    defer map.deinit();

    try map.connect(file);

    // when
    const rslt = try map.write(expected);

    const actual = try alloc.alloc(u8, rslt.len);
    defer alloc.free(actual);

    _ = try map.read(rslt.idx, actual);

    // then
    try testing.expect(map.getCount() == 1);
    try testing.expectEqualStrings(expected, actual);
}

test "MMap append" {
    const FileUtils = @import("file.zig");

    const row = extern struct {
        key: u64,
        value: [*c]const u8,
    };

    const testing = std.testing;
    var alloc = testing.allocator;
    const testdir = testing.tmpDir(.{});
    const pathname = try testdir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testdir.dir.deleteDir(pathname) catch {};
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "append.dat" });
    defer alloc.free(filename);
    var file = try FileUtils.openWithCapacity(filename, std.mem.page_size);
    defer file.close();

    // given
    const key = std.hash.Murmur2_64.hash("__key__");
    const expected: row = .{ .key = key, .value = "__value__" };

    var map = try MMap(row).init(alloc, std.mem.page_size);
    defer alloc.destroy(map);
    defer map.deinit();

    try map.connect(file);

    // when
    try map.append(row{ .key = std.hash.Murmur2_64.hash("__key_0__"), .value = "__value0__" });
    try map.append(row{ .key = std.hash.Murmur2_64.hash("__key_1__"), .value = "__value1__" });
    try map.append(row{ .key = std.hash.Murmur2_64.hash("__key_2__"), .value = "__value2__" });
    try map.append(expected);

    const actual = try map.read(3);

    // then
    try testing.expect(map.count == 4);
    try testing.expect(expected.key == actual.key);
}

test "MMap insert" {
    const FileUtils = @import("file.zig");

    const Row = extern struct {
        key: u64,
        value: [*c]const u8,
    };

    const testing = std.testing;
    var alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "insert.dat" });
    defer alloc.free(filename);
    var file = try FileUtils.openWithCapacity(filename, std.mem.page_size);
    defer file.close();

    // given
    const key = std.hash.Murmur2_64.hash("__key__");
    const expected: Row = .{ .key = key, .value = "__expected__" };

    var map = try MMap(Row).init(alloc, std.mem.page_size);
    defer alloc.destroy(map);
    defer map.deinit();

    try map.connect(file);

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

    const actual = try map.read(1);

    // then
    try testing.expect(map.count == 4);
    try testing.expect(expected.key == actual.key);
}
