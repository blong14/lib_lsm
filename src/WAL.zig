const std = @import("std");

const Iterator = @import("iterator.zig").Iterator;
const KV = @import("KV.zig");
const Block = @import("Block.zig");

const Allocator = std.mem.Allocator;
const Mutex = std.Thread.Mutex;

const MB = 1024 * 1024;

pub const SegmentError = error{
    FileOpenFailed,
    FileSetSizeFailed,
    FileStatFailed,
    MemoryMapFailed,
    BlockAllocationFailed,
    BlockInitFailed,
    NoSpaceLeft,
    IndexOutOfBounds,
};

pub const WalError = error{
    SegmentAllocationFailed,
    SegmentInitFailed,
    SegmentListInitFailed,
    IndexOutOfBounds,
    NoSpaceLeft,
};

pub const WalConfig = struct {
    Dir: []const u8,
    Segment: struct {
        MaxStoreBytes: u64 = 16 * MB,
    } = .{},
};

pub const Segment = struct {
    file: std.fs.File,
    block: Block,
    id: u64,
    max_bytes: u64,
    count: u64 = 0,
    size: u64 = 0,

    const Self = @This();

    pub fn init(alloc: Allocator, id: u64, conf: WalConfig) !Self {
        const wal_path = try std.fmt.allocPrint(
            alloc,
            "{s}/wal_{d}.dat",
            .{ conf.Dir, id },
        );
        defer alloc.free(wal_path);

        const wal_file = try std.fs.cwd().createFile(wal_path, .{ .read = true, .truncate = false });
        try wal_file.setEndPos(conf.Segment.MaxStoreBytes);
        errdefer wal_file.close();

        const stat = try wal_file.stat();
        const file_size = stat.size;

        const stream: []align(std.heap.page_size_min) u8 = try std.posix.mmap(
            null,
            file_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED, .ANONYMOUS = false },
            wal_file.handle,
            0,
        );
        errdefer std.posix.munmap(stream);

        const max_offset = @divTrunc(conf.Segment.MaxStoreBytes, 4);

        return .{
            .id = id,
            .file = wal_file,
            .block = .init(stream, .{ .max_offset_bytes = max_offset }),
            .max_bytes = file_size,
        };
    }

    pub fn deinit(self: *Self) void {
        self.file.sync() catch {};
        self.file.close();
        self.* = undefined;
    }

    pub fn write(self: *Self, kv: KV) !void {
        const kv_size = kv.len();
        if ((self.size + kv_size) >= self.max_bytes) {
            return SegmentError.NoSpaceLeft;
        }

        _ = self.block.write(kv) catch |err| switch (err) {
            error.NoSpaceLeft, error.NoOffsetSpaceLeft => {
                return SegmentError.NoSpaceLeft;
            },
            else => return err,
        };

        self.count += 1;
        self.size += kv_size;
    }

    pub fn read(self: Self, idx: u64) !KV {
        if (idx >= self.count) {
            return SegmentError.IndexOutOfBounds;
        }

        return self.block.read(idx) catch |err| switch (err) {
            error.IndexOutOfBounds, error.InvalidOffset => {
                return SegmentError.IndexOutOfBounds;
            },
            else => return err,
        };
    }

    pub fn getCount(self: Self) u64 {
        return self.count;
    }
};

conf: WalConfig,
mtx: std.Thread.Mutex = .{},
active_segment: *Segment,
segments: std.ArrayList(*Segment),

pub const WAL = @This();

pub fn init(alloc: Allocator, conf: WalConfig) !WAL {
    const active_segment = try alloc.create(Segment);
    errdefer alloc.destroy(active_segment);

    active_segment.* = try Segment.init(alloc, 1, conf);

    const segments = try std.ArrayList(*Segment).initCapacity(alloc, 256);

    return .{
        .conf = conf,
        .active_segment = active_segment,
        .segments = segments,
    };
}

pub fn deinit(self: *WAL, alloc: Allocator) !void {
    try self.active_segment.file.sync();
    self.active_segment.deinit();
    alloc.destroy(self.active_segment);
    for (self.segments.items) |segment| {
        segment.deinit();
        alloc.destroy(segment);
    }
    self.segments.deinit(alloc);
    self.* = undefined;
}

pub fn write(self: *WAL, alloc: Allocator, kv: KV) !void {
    self.mtx.lock();
    defer self.mtx.unlock();

    self.active_segment.write(kv) catch |err| switch (err) {
        SegmentError.NoSpaceLeft => {
            self.active_segment.file.sync() catch {};

            try self.segments.append(alloc, self.active_segment);

            const current_id = self.active_segment.id;

            const active_segment = try alloc.create(Segment);
            active_segment.* = try Segment.init(
                alloc,
                current_id + 1,
                self.conf,
            );

            self.active_segment = active_segment;
            try self.active_segment.write(kv);
        },
        else => return err,
    };
}

fn read(self: *WAL, idx: u64) !KV {
    self.mtx.lock();
    defer self.mtx.unlock();

    var current_idx: u64 = 0;
    for (self.segments.items) |segment| {
        const segment_count = segment.getCount();
        if (idx >= current_idx and idx < current_idx + segment_count) {
            const local_idx = idx - current_idx;
            return try segment.read(local_idx);
        }
        current_idx += segment_count;
    }

    const active_count = self.active_segment.getCount();
    if (idx >= current_idx and idx < current_idx + active_count) {
        const local_idx = idx - current_idx;
        return self.active_segment.read(local_idx);
    }

    return WalError.IndexOutOfBounds;
}

const WalIter = struct {
    alloc: Allocator,
    wal: *WAL,
    nxt: usize = 0,

    pub fn deinit(ctx: *anyopaque) void {
        const self: *WalIter = @ptrCast(@alignCast(ctx));
        self.alloc.destroy(self);
    }

    pub fn next(ctx: *anyopaque) ?KV {
        const self: *WalIter = @ptrCast(@alignCast(ctx));

        const kv = self.wal.read(self.nxt) catch return null;

        self.*.nxt += 1;

        return kv;
    }
};

pub fn iterator(self: *WAL, alloc: Allocator) !Iterator(KV) {
    const wit = try alloc.create(WalIter);
    wit.* = .{ .alloc = alloc, .wal = self };
    return Iterator(KV).init(wit, WalIter.next, WalIter.deinit);
}

test WAL {
    const testing = std.testing;
    const alloc = testing.allocator;

    const test_dir = testing.tmpDir(.{});
    const pathname = try test_dir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer test_dir.dir.deleteTree(pathname) catch {};

    // given
    var st = try WAL.init(alloc, .{ .Dir = pathname });
    defer st.deinit(alloc) catch unreachable;

    const expected: []const u8 = "__value__";

    // when
    var kv = try KV.init(alloc, "__key__", expected);
    defer kv.deinit(alloc);

    try st.write(alloc, kv);

    var iter = try st.iterator(alloc);
    defer iter.deinit();

    const actual = iter.next();

    try testing.expectEqualStrings(expected, actual.?.value);
}
