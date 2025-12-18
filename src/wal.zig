const std = @import("std");

const file_utils = @import("file.zig");
const Iterator = @import("iterator.zig").Iterator;
const KV = @import("kv.zig").KV;
const Block = @import("Block.zig");

const Allocator = std.mem.Allocator;
const Mutex = std.Thread.Mutex;

const Endian = std.builtin.Endian.little;
const MB = 1024 * 1024;
const PageSize = std.heap.pageSize();
const Sep = &[_]u8{'\n'};

const assert = std.debug.assert;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;

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
    conf: WalConfig,
    file: std.fs.File,
    block: *Block,
    stream: []u8,
    id: u64,
    max_bytes: u64,
    pos: u64 = 0,
    count: u64 = 0,
    size: u64 = 0,
    connected: bool = false,
    mutable: bool = true,

    const Self = @This();

    pub fn init(alloc: Allocator, id: u64, conf: WalConfig) SegmentError!Self {
        const wal_path = try std.fmt.allocPrint(
            alloc,
            "{s}/wal_{d}.dat",
            .{ conf.Dir, id },
        );
        defer alloc.free(wal_path);

        const wal_file = file_utils.open(wal_path) catch {
            return SegmentError.FileOpenFailed;
        };
        errdefer wal_file.close();

        wal_file.setEndPos(conf.Segment.MaxStoreBytes) catch {
            return SegmentError.FileSetSizeFailed;
        };

        const stat = wal_file.stat() catch {
            return SegmentError.FileStatFailed;
        };
        const file_size = stat.size;

        const stream = std.posix.mmap(
            null,
            file_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED, .ANONYMOUS = false },
            wal_file.handle,
            0,
        ) catch {
            return SegmentError.MemoryMapFailed;
        };
        errdefer std.posix.munmap(stream);

        const max_offset = @divTrunc(conf.Segment.MaxStoreBytes, 4);

        const blck = alloc.create(Block) catch {
            return SegmentError.BlockAllocationFailed;
        };
        errdefer alloc.destroy(blck);

        blck.* = Block.init(stream, .{ .max_offset_bytes = max_offset }) catch {
            return SegmentError.BlockInitFailed;
        };

        return .{
            .file = wal_file,
            .stream = stream,
            .block = blck,
            .conf = conf,
            .connected = true,
            .id = id,
            .max_bytes = file_size,
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        self.file.sync() catch {};
        self.file.close();
        std.posix.munmap(self.stream);
        alloc.destroy(self.block);
        self.* = undefined;
    }

    pub fn write(self: *Self, kv: KV) SegmentError!void {
        const kv_size = kv.len();
        if ((self.size + kv_size) >= self.max_bytes) {
            return SegmentError.NoSpaceLeft;
        }

        _ = self.block.write(kv) catch {
            return SegmentError.NoSpaceLeft;
        };

        self.count += 1;
        self.size += kv_size;
    }

    pub fn read(self: Self, idx: u64) SegmentError!KV {
        if (idx >= self.count) {
            return SegmentError.IndexOutOfBounds;
        }

        return self.block.read(idx) catch {
            return SegmentError.IndexOutOfBounds;
        };
    }

    pub fn getCount(self: Self) u64 {
        return self.count;
    }
};

pub const WAL = struct {
    conf: WalConfig,
    mtx: Mutex,
    active_segment: *Segment,
    segments: std.ArrayList(*Segment),

    const Self = @This();

    pub fn init(alloc: Allocator, conf: WalConfig) WalError!Self {
        const active_segment = alloc.create(Segment) catch {
            return WalError.SegmentAllocationFailed;
        };
        errdefer alloc.destroy(active_segment);

        active_segment.* = Segment.init(alloc, 1, conf) catch {
            return WalError.SegmentInitFailed;
        };

        const segments = std.ArrayList(*Segment).initCapacity(alloc, 256) catch {
            active_segment.deinit(alloc);
            return WalError.SegmentListInitFailed;
        };

        return .{
            .mtx = Mutex{},
            .conf = conf,
            .active_segment = active_segment,
            .segments = segments,
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) !void {
        try self.active_segment.file.sync();
        self.active_segment.deinit(alloc);
        alloc.destroy(self.active_segment);
        for (self.segments.items) |segment| {
            segment.deinit(alloc);
            alloc.destroy(segment);
        }
        self.segments.deinit(alloc);
        self.* = undefined;
    }

    pub fn write(self: *Self, alloc: Allocator, kv: KV) WalError!void {
        self.mtx.lock();
        defer self.mtx.unlock();

        self.active_segment.write(kv) catch |err| switch (err) {
            SegmentError.NoSpaceLeft => {
                self.active_segment.file.sync() catch {};

                self.segments.append(self.active_segment) catch {
                    return WalError.SegmentListInitFailed;
                };

                const current_id = self.active_segment.id;

                const active_segment = alloc.create(Segment) catch {
                    return WalError.SegmentAllocationFailed;
                };
                active_segment.* = Segment.init(
                    alloc,
                    current_id + 1,
                    self.conf,
                ) catch {
                    alloc.destroy(active_segment);
                    return WalError.SegmentInitFailed;
                };

                self.active_segment = active_segment;
                self.active_segment.write(kv) catch {
                    return WalError.NoSpaceLeft;
                };
            },
            else => return WalError.NoSpaceLeft,
        };
    }

    fn read(self: *Self, idx: u64) WalError!KV {
        self.mtx.lock();
        defer self.mtx.unlock();

        var current_idx: u64 = 0;
        for (self.segments.items) |segment| {
            const segment_count = segment.getCount();
            if (idx >= current_idx and idx < current_idx + segment_count) {
                const local_idx = idx - current_idx;
                return segment.read(local_idx) catch {
                    return WalError.IndexOutOfBounds;
                };
            }
            current_idx += segment_count;
        }

        const active_count = self.active_segment.getCount();
        if (idx >= current_idx and idx < current_idx + active_count) {
            const local_idx = idx - current_idx;
            return self.active_segment.read(local_idx) catch {
                return WalError.IndexOutOfBounds;
            };
        }

        return WalError.IndexOutOfBounds;
    }

    const WalIter = struct {
        alloc: Allocator,
        wal: *WAL = undefined,
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

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        const wit = try alloc.create(WalIter);
        wit.* = .{ .alloc = alloc, .wal = self };
        return Iterator(KV).init(wit, WalIter.next, WalIter.deinit);
    }
};

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
    var kv = try KV.initOwned(alloc, "__key__", expected);
    defer kv.deinit(alloc);

    try st.write(alloc, kv);

    var iter = try st.iterator(alloc);
    defer iter.deinit();

    const actual = iter.next();

    try testing.expectEqualStrings(expected, actual.?.value);
}
