const std = @import("std");

const Block = @import("block.zig").Block;
const file_utils = @import("file.zig");
const Iterator = @import("iterator.zig").Iterator;
const KV = @import("kv.zig").KV;
const MMap = @import("mmap.zig").AppendOnlyMMap;

const Allocator = std.mem.Allocator;
const BufferedWriter = std.io.BufferedWriter;
const FixedBuffer = std.io.FixedBufferStream;
const Mutex = std.Thread.Mutex;

const Endian = std.builtin.Endian.little;
const MB = 1024 * 1024;
const PageSize = std.mem.page_size;

const bufferedWriter = std.io.bufferedWriter;
const fixedBufferStream = std.io.fixedBufferStream;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;

pub const WalConfig = struct {
    Dir: []const u8,
    Segment: struct {
        MaxStoreBytes: u64 = MB,
    } = .{},
};

pub const Store = struct {
    file: std.fs.File,
    stream: *MMap,
    block: *Block,
    count: u64 = 0,
    size: u64 = 0,
    connected: bool = false,
    mutable: bool = true,

    const Self = @This();

    pub fn init() Self {
        return .{
            .block = undefined,
            .stream = undefined,
            .file = undefined,
        };
    }

    pub fn open(self: *Self, alloc: Allocator, file: std.fs.File) !void {
        if (self.connected) {
            return;
        }

        const stat = try file.stat();
        const file_size = stat.size;

        var stream = try MMap.init(alloc, file_size);
        errdefer {
            stream.deinit();
            alloc.destroy(stream);
        }

        try stream.connect(file, 0);
        stream.buf.reset();

        const reader = stream.buf.reader();

        const first_key_len = try reader.readInt(u64, Endian);
        stream.buf.reset();

        var blck: *Block = undefined;
        if (first_key_len > 0) {
            self.*.mutable = false;
            std.debug.print("init wal block data\n", .{});
            blck = try Block.initFromData(alloc, stream.buf.buffer);
        } else {
            blck = try alloc.create(Block);
            blck.* = Block.init(alloc, stream.buf.buffer);
        }

        self.*.block = blck;
        self.*.connected = true;
        self.*.file = file;
        self.*.stream = stream;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        self.block.deinit();
        alloc.destroy(self.block);
        self.stream.deinit();
        alloc.destroy(self.stream);
        self.file.close();
        self.* = undefined;
    }

    pub fn getCount(self: Self) u64 {
        return self.count;
    }

    pub fn read(self: *const Self, idx: u64) !KV {
        return self.block.read(idx);
    }

    pub fn append(self: *Self, data: KV) !void {
        _ = try self.block.write(data);
        self.size += data.len();
        self.count += 1;
    }
};

pub const Segment = struct {
    conf: WalConfig,
    store: Store,
    id: u64,
    max_bytes: u64,

    const Self = @This();

    pub fn init(alloc: Allocator, id: u64, conf: WalConfig) !Self {
        const wal_path = try std.fmt.allocPrint(
            alloc,
            "{s}/wal_{d}.dat",
            .{
                conf.Dir,
                id,
            },
        );
        defer alloc.free(wal_path);

        const wal_file = try file_utils.open(wal_path);
        try wal_file.setEndPos(conf.Segment.MaxStoreBytes);

        const stat = try wal_file.stat();

        var store = Store.init();
        try store.open(alloc, wal_file);

        return .{
            .conf = conf,
            .id = id,
            .max_bytes = stat.size,
            .store = store,
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        self.store.deinit(alloc);
        self.* = undefined;
    }

    pub fn write(self: *Self, kv: KV) !void {
        if (kv.len() >= self.max_bytes) {
            return error.NoSpaceLeft;
        }

        _ = try self.store.append(kv);
    }

    pub fn read(self: Self, idx: u64) ?[]const u8 {
        if (idx >= self.store.getCount()) return null;

        return self.store.read(idx) catch null;
    }
};

pub const WAL = struct {
    conf: WalConfig,
    mtx: Mutex,
    active_segment: *Segment,
    segments: std.ArrayList(*Segment),

    const Self = @This();

    pub fn init(alloc: Allocator, conf: WalConfig) !Self {
        const active_segment = try alloc.create(Segment);
        active_segment.* = try Segment.init(alloc, 1, conf);

        return .{
            .mtx = Mutex{},
            .conf = conf,
            .active_segment = active_segment,
            .segments = std.ArrayList(*Segment).init(alloc),
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) !void {
        self.active_segment.deinit(alloc);
        alloc.destroy(self.active_segment);
        for (self.segments.items) |segment| {
            segment.deinit(alloc);
            alloc.destroy(segment);
        }
        self.segments.deinit();
        self.* = undefined;
    }

    pub fn write(self: *Self, alloc: Allocator, kv: KV) !void {
        self.mtx.lock();
        defer self.mtx.unlock();

        self.active_segment.write(kv) catch |err| switch (err) {
            error.NoSpaceLeft => {
                try self.segments.append(self.active_segment);

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

    fn read(self: *Self, idx: u64) !KV {
        self.mtx.lock();
        defer self.mtx.unlock();

        var current_idx: u64 = 0;
        for (self.segments.items) |segment| {
            const segment_count = segment.store.getCount();
            if (idx >= current_idx and idx < current_idx + segment_count) {
                const local_idx = idx - current_idx;
                return segment.store.read(local_idx);
            }
            current_idx += segment_count;
        }

        const active_count = self.active_segment.store.getCount();
        if (idx >= current_idx and idx < current_idx + active_count) {
            const local_idx = idx - current_idx;
            return self.active_segment.store.read(local_idx);
        }

        return error.IndexOutOfBounds;
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

            const kv = self.wal.read(self.nxt) catch |err| {
                std.log.err("wal iter error {s}", .{@errorName(err)});
                return null;
            };

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
