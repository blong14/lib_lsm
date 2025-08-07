const std = @import("std");

const File = @import("file.zig");
const MMap = @import("mmap.zig").MMap;

const Allocator = std.mem.Allocator;
const BufferedWriter = std.io.BufferedWriter;
const Endian = std.builtin.Endian.little;
const FixedBuffer = std.io.FixedBufferStream;
const Mutex = std.Thread.Mutex;
const PageSize = std.mem.page_size;

const bufferedWriter = std.io.bufferedWriter;
const fixedBufferStream = std.io.fixedBufferStream;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;

pub const WalConfig = struct {
    Dir: std.fs.Dir,
    Segment: struct {
        MaxStoreBytes: u64 = PageSize,
        MaxIndexBytes: u64 = PageSize,
        InitialOffset: u64 = 0,
    } = .{},
};

pub const Store = struct {
    mtx: Mutex,
    file: std.fs.File,
    buf: BufferedWriter(PageSize, std.fs.File.Writer),
    size: u64,

    const Self = @This();

    pub fn init(f: std.fs.File) !Self {
        const stat = try f.stat();
        const writer = bufferedWriter(f.writer());
        return .{
            .mtx = Mutex{},
            .file = f,
            .buf = writer,
            .size = stat.size,
        };
    }

    pub fn close(self: *Self) !void {
        self.mtx.lock();
        try self.buf.flush();
        self.file.close();
        self.mtx.unlock();
        self.* = undefined;
    }

    pub fn read(self: *Self, pos: u64) ![]const u8 {
        self.mtx.lock();
        defer self.mtx.unlock();

        try self.buf.flush();

        const stream = fixedBufferStream(self.file.reader());
        stream.seekTo(pos);

        var reader = stream.reader();

        const len = try reader.readInt(u64, Endian);

        var out: [PageSize]u8 = undefined;
        try reader.readAtLeast(out, len);

        return out[0..len];
    }

    pub fn append(self: *Self, data: []const u8) !struct { offset: u64, len: usize } {
        self.mtx.lock();
        defer self.mtx.unlock();

        var len: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        writeInt(std.math.ByteAlignedInt(u64), &len, data.len, Endian);

        const pos = self.buf.end;

        var byts = try self.buf.write(&len);
        byts += try self.buf.write(data);

        self.size += byts;

        return .{ .len = byts, .offset = pos };
    }
};

pub const Index = struct {
    const Row = extern struct {
        offset: u64,
        pos: u64,
    };

    file: std.fs.File,
    mmap: *MMap(Row),
    size: u64,

    const Self = @This();

    pub fn init(alloc: Allocator, f: std.fs.File) !Self {
        const stat = try f.stat();

        const data = try MMap(Row).init(alloc, stat.size);
        try data.connect(f);

        return .{
            .file = f,
            .mmap = data,
            .size = stat.size,
        };
    }

    pub fn deinit(self: *Self) !void {
        try self.close();
        self.* = undefined;
    }

    pub fn close(self: *Self) !void {
        try self.file.sync();
        defer self.file.close();

        self.mmap.deinit();
    }

    pub fn read(self: *Self, in: u64) !Row {
        try self.mmap.read(in);
    }

    pub fn write(self: *Self, off: u64, pos: u64) !void {
        const row: Row = .{ .offset = off, .pos = pos };
        try self.mmap.append(row);
    }
};

pub const Segment = struct {
    conf: WalConfig,
    index: Index,
    store: Store,

    const Self = @This();

    pub fn init(alloc: Allocator, conf: WalConfig) !Self {
        const idx_file = try conf.Dir.createFile("segment_idx.dat", .{ .read = true });
        errdefer idx_file.close();

        try idx_file.setEndPos(conf.Segment.MaxIndexBytes);

        const idx = try Index.init(alloc, idx_file);

        const store_file = try conf.Dir.createFile("segment_store.dat", .{ .read = true });
        errdefer store_file.close();

        try store_file.setEndPos(conf.Segment.MaxStoreBytes);

        const store = try Store.init(store_file);

        return .{
            .conf = conf,
            .index = idx,
            .store = store,
        };
    }

    pub fn deinit(self: *Self) !void {
        try self.index.deinit();
        try self.store.close();
        self.* = undefined;
    }

    pub fn write(self: *Self, data: []const u8) !void {
        const resp = try self.store.append(data);
        try self.index.write(resp.offset, resp.len);
    }
};

pub const XWAL = struct {
    conf: WalConfig,
    active_segment: Segment,
    segments: std.ArrayList(Segment),

    const Self = @This();

    pub fn init(alloc: Allocator, conf: WalConfig) !Self {
        return .{
            .conf = conf,
            .active_segment = try Segment.init(alloc, conf),
            .segments = std.ArrayList(Segment).init(alloc),
        };
    }

    pub fn deinit(self: *Self) !void {
        try self.active_segment.deinit();
        self.segments.deinit();
        self.* = undefined;
    }

    pub fn write(self: *Self, alloc: Allocator, key: u64, data: []const u8) !void {
        _ = key;
        self.active_segment.write(data) catch |err| switch (err) {
            error.WriteError => {
                try self.segments.append(self.active_segment);
                self.active_segment = try Segment.init(alloc, self.conf);
            },
            else => return err,
        };
    }
};

test XWAL {
    const testing = std.testing;
    var alloc = testing.allocator;

    const test_dir = testing.tmpDir(.{});
    const pathname = try test_dir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    // defer test_dir.dir.deleteTree(pathname) catch {};

    std.debug.print("{s}\n", .{pathname});

    // given
    var st = try XWAL.init(alloc, .{ .Dir = test_dir.dir });
    defer st.deinit() catch unreachable;

    // when
    const key: u64 = std.hash.Murmur2_64.hash("__key__");
    const expected: []const u8 = "__value__";
    try st.write(alloc, key, expected);

    // then
    //var iter = st.iterator();
    //const actual = iter.next();
    //try testing.expectEqualStrings(expected, actual.?.value);
}

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
        const data = try MMap(Row).init(alloc, capacity);
        try data.connect(file);
        const wal = try alloc.create(Self);
        wal.* = .{ .alloc = alloc, .data = data, .file = file };
        return wal;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
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
