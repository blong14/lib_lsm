const std = @import("std");

const opts = @import("opts.zig");
const iter = @import("iterator.zig");

const Allocator = std.mem.Allocator;

const KV = @import("KV.zig");
const Memtable = @import("Memtable.zig");
const WAL = @import("WAL.zig");
const Iterator = iter.Iterator;
const SSTable = @import("SSTable.zig");

const LsmState = struct {
    lock: std.Thread.RwLock = .{},
    write_buffer: *Memtable = undefined,
    read_buffer: *Memtable = undefined,
    wal: *WAL = undefined,

    fn deinit(state: *LsmState, alloc: Allocator) void {
        alloc.destroy(state.write_buffer);
        alloc.destroy(state.read_buffer);
        state.* = undefined;
    }

    fn freeze(state: *LsmState) void {
        return state.lock.lock();
    }

    fn acquire(state: *LsmState) bool {
        return state.lock.tryLockShared();
    }

    pub fn release(state: *LsmState) void {
        state.lock.unlockShared();
    }
};

Opts: opts.Opts,
state_lease: std.Thread.RwLock = .{},
state: *LsmState,
snapshots: std.ArrayList(*LsmState),
wal: *WAL,

const Database = @This();

pub fn init(alloc: Allocator, o: opts.Opts) !Database {
    const write_buffer = try alloc.create(Memtable);
    errdefer alloc.destroy(write_buffer);

    write_buffer.* = try Memtable.init(0);

    const read_buffer = try alloc.create(Memtable);
    errdefer alloc.destroy(read_buffer);

    read_buffer.* = try Memtable.init(0);

    const wal = try alloc.create(WAL);
    errdefer alloc.destroy(wal);

    wal.* = try WAL.init(alloc, .{ .Dir = o.data_dir });

    const state = try alloc.create(LsmState);
    errdefer alloc.destroy(state);

    state.* = .{
        .write_buffer = write_buffer,
        .read_buffer = read_buffer,
        .wal = wal,
    };

    return .{
        .Opts = o,
        .state = state,
        .snapshots = try std.ArrayList(*LsmState).initCapacity(alloc, 256),
        .wal = wal,
    };
}

pub fn deinit(self: *Database, alloc: Allocator) void {
    self.state.deinit(alloc);
    alloc.destroy(self.state);

    for (self.snapshots.items) |state| {
        state.deinit(alloc);
        alloc.destroy(state);
    }
    self.snapshots.deinit(alloc);

    self.wal.deinit(alloc) catch unreachable;
    alloc.destroy(self.wal);

    self.* = undefined;
}

pub fn open(self: *Database, alloc: Allocator) !void {
    const data_dir = try std.fs.cwd().openDir(self.Opts.data_dir, .{ .iterate = true });
    var dir_iter = data_dir.iterate();

    while (try dir_iter.next()) |entry| {
        if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".mtab")) {
            var file_path_buf: [256]u8 = undefined;
            const file_path = try std.fmt.bufPrint(
                &file_path_buf,
                "{s}/{s}",
                .{ self.Opts.data_dir, entry.name },
            );

            var read_buffer = try alloc.create(Memtable);
            read_buffer.deserialize(alloc, file_path) catch |err|
                @panic(@errorName(err));

            self.state_lease.lock();
            self.state.read_buffer = read_buffer;
            self.state_lease.unlock();

            std.log.info("serialized memtable {d} from {s} w/ {d} bytes", .{
                read_buffer.getId(),
                file_path,
                read_buffer.size(),
            });

            break;
        }
    }
}

pub fn close(self: *Database, alloc: Allocator) !void {
    try self.flush(alloc);

    self.state_lease.lock();

    var buf = self.state.read_buffer;
    buf.freeze();
    buf.flush();

    var buffer: [256]u8 = undefined;
    const file_path = std.fmt.bufPrint(&buffer, "{s}/memtable_{d}.mtab", .{
        self.Opts.data_dir,
        buf.getId(),
    }) catch unreachable;

    try buf.serialize(alloc, file_path);
}

pub fn transaction(self: *Database) !?*LsmState {
    const state: *LsmState = blk: {
        self.state_lease.lockShared();
        defer self.state_lease.unlockShared();
        break :blk self.state;
    };

    if (state.acquire()) {
        return state;
    } else {
        return error.InvalidLsmState;
    }
}

pub fn read(self: *Database, alloc: Allocator, key: []const u8) !?KV {
    _ = alloc;
    if (key.len == 0) return null;

    if (try self.transaction()) |txn| {
        defer txn.release();

        if (try txn.write_buffer.get(key)) |kv| {
            return kv;
        }

        return try txn.read_buffer.get(key);
    } else {
        return null;
    }
}

pub fn write(self: *Database, alloc: Allocator, kv: KV) !void {
    if (kv.key.len == 0) {
        return error.InvalidKeyData;
    }
    if (kv.value.len == 0) {
        return error.InvalidValueData;
    }

    if (try self.transaction()) |txn| {
        defer txn.release();

        try txn.write_buffer.put(kv);
        try txn.wal.write(alloc, kv);
    }
}

pub fn flush(self: *Database, alloc: Allocator) !void {
    var prev: *LsmState = blk: {
        self.state_lease.lockShared();
        defer self.state_lease.unlockShared();
        break :blk self.state;
    };

    const write_buffer = try alloc.create(Memtable);
    errdefer alloc.destroy(write_buffer);

    if (prev.acquire()) {
        defer prev.release();

        if (prev.write_buffer.size() == 0) return;

        write_buffer.* = try Memtable.init(prev.write_buffer.getId() + 1);
        errdefer write_buffer.deinit();

        var sstable = try SSTable.createFile(alloc, 0, self.Opts);

        var it = try prev.write_buffer.iterator(alloc);
        defer it.deinit();

        while (it.next()) |item| {
            try prev.read_buffer.put(item);

            _ = sstable.write(item) catch |err| switch (err) {
                error.BufferFull => {
                    sstable.deinit();
                    alloc.destroy(sstable);

                    sstable = try SSTable.createFile(alloc, 0, self.Opts);
                },
                else => return err,
            };
        }
    } else {
        alloc.destroy(write_buffer);
        return;
    }

    self.state = blk: {
        self.state_lease.lock();
        defer self.state_lease.unlock();

        prev.freeze();

        prev.write_buffer.freeze();

        const nxt = try alloc.create(LsmState);
        errdefer alloc.destroy(nxt);

        nxt.* = .{
            .wal = prev.wal,
            .write_buffer = write_buffer,
            .read_buffer = prev.read_buffer,
        };

        try self.snapshots.append(alloc, prev);

        break :blk nxt;
    };
}

pub fn compare(a: KV, b: KV) std.math.Order {
    const key_order = std.mem.order(u8, a.key, b.key);
    if (key_order == .eq) {
        return if (a.timestamp < b.timestamp) .lt else .gt;
    }

    return key_order;
}

pub fn userKeyCompare(a: KV, b: KV) std.math.Order {
    return std.mem.order(u8, a.key, b.key);
}

const MergeIteratorWrapper = struct {
    alloc: Allocator,
    state: *LsmState,
    merger: *iter.MergeIterator(KV, compare),
    it: Iterator(KV),

    pub fn deinit(ctx: *anyopaque) void {
        var wrapper: *@This() = @ptrCast(@alignCast(ctx));
        wrapper.it.deinit();
        wrapper.alloc.destroy(wrapper.merger);
        wrapper.state.release();
        wrapper.alloc.destroy(wrapper);
    }

    pub fn next(ctx: *anyopaque) ?KV {
        var wrapper: *@This() = @ptrCast(@alignCast(ctx));
        return wrapper.it.next();
    }
};

pub fn iterator(self: *Database, alloc: Allocator) !Iterator(KV) {
    var merger = try alloc.create(iter.MergeIterator(KV, compare));
    errdefer alloc.destroy(merger);

    merger.* = try iter.MergeIterator(KV, compare).init(alloc);

    const state: *LsmState = blk: {
        self.state_lease.lockShared();
        defer self.state_lease.unlockShared();
        break :blk self.state;
    };

    if (state.acquire()) {
        var hot_iter = try state.write_buffer.iterator(alloc);
        errdefer hot_iter.deinit();

        try merger.add(hot_iter);

        var cold_iter = try state.read_buffer.iterator(alloc);
        errdefer cold_iter.deinit();

        try merger.add(cold_iter);
    }

    const wrapper = try alloc.create(MergeIteratorWrapper);
    wrapper.* = .{
        .alloc = alloc,
        .state = state,
        .merger = merger,
        .it = merger.iterator(),
    };

    return Iterator(KV).init(wrapper, MergeIteratorWrapper.next, MergeIteratorWrapper.deinit);
}

const ScanWrapper = struct {
    alloc: Allocator,
    scanner: *iter.ScanIterator(KV, userKeyCompare),
    it: Iterator(KV),

    const Wrapper = @This();

    pub fn deinit(ctx: *anyopaque) void {
        const sw: *Wrapper = @ptrCast(@alignCast(ctx));
        sw.it.deinit();
        if (sw.scanner.start) |start| {
            @constCast(&start).deinit(sw.alloc);
        }
        if (sw.scanner.end) |end| {
            @constCast(&end).deinit(sw.alloc);
        }
        sw.alloc.destroy(sw.scanner);
        sw.alloc.destroy(sw);
    }

    pub fn next(ctx: *anyopaque) ?KV {
        const sw: *Wrapper = @ptrCast(@alignCast(ctx));
        return sw.it.next();
    }
};

pub fn scan(
    self: *Database,
    alloc: Allocator,
    start_key: []const u8,
    end_key: []const u8,
) !Iterator(KV) {
    var start = try KV.init(alloc, start_key, "");
    errdefer start.deinit(alloc);

    var end = try KV.init(alloc, end_key, "");
    errdefer end.deinit(alloc);

    var si = try alloc.create(iter.ScanIterator(KV, userKeyCompare));
    errdefer alloc.destroy(si);

    var base_iter = try self.iterator(alloc);
    errdefer base_iter.deinit();

    si.* = iter.ScanIterator(KV, userKeyCompare).init(base_iter, start, end);

    const wrapper = try alloc.create(ScanWrapper);
    wrapper.* = .{
        .alloc = alloc,
        .scanner = si,
        .it = si.iterator(),
    };

    return Iterator(KV).init(wrapper, ScanWrapper.next, ScanWrapper.deinit);
}

test "KV compare" {
    const Order = std.math.Order;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    // given
    const kv1 = try KV.init(alloc, "key1", "value1");
    const kv2 = try KV.init(alloc, "key2", "value2");
    var kv3 = try KV.init(alloc, "key1", "different_value");
    kv3.timestamp = kv1.timestamp + 1;

    // then
    try std.testing.expectEqual(Order.lt, compare(kv1, kv2));
    try std.testing.expectEqual(Order.gt, compare(kv2, kv1));
    try std.testing.expectEqual(Order.lt, compare(kv1, kv3));
    try std.testing.expectEqual(Order.lt, compare(kv3, kv2));
    try std.testing.expectEqual(Order.gt, compare(kv3, kv1));

    try std.testing.expectEqual(Order.eq, userKeyCompare(kv1, kv3));
    try std.testing.expect(!std.mem.eql(u8, kv1.value, kv3.value));
}
