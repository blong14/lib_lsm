const std = @import("std");

const file_utils = @import("file.zig");
const keyvalue = @import("kv.zig");
const iter = @import("iterator.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tbm = @import("tablemap.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;
const File = std.fs.File;
const Mutex = std.Thread.Mutex;

const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const Opts = options.Opts;
const SkipList = skl.SkipList;
const SSTable = sst.SSTable;

const KeyValueSkipList = SkipList([]const u8, keyvalue.decode, keyvalue.encode);

const assert = std.debug.assert;
const print = std.debug.print;

var mtx: Mutex = .{};

pub const Memtable = struct {
    init_alloc: Allocator,
    cap: usize,
    id: u64,
    opts: Opts,

    byte_count: AtomicValue(usize),
    data: AtomicValue(*KeyValueSkipList),
    mutable: AtomicValue(bool),
    isFlushed: AtomicValue(bool),
    sstable: AtomicValue(?*SSTable),

    // Memory ordering constants for better readability
    const relaxed = .monotonic;
    const acquire = .acquire;
    const release = .release;
    const seq_cst = .seq_cst;

    const Self = @This();

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity;

        const map = try alloc.create(KeyValueSkipList);
        map.* = try KeyValueSkipList.init(alloc);

        const mtable = try alloc.create(Self);
        mtable.* = .{
            .init_alloc = alloc,
            .cap = cap,
            .id = id,
            .opts = opts,
            .byte_count = AtomicValue(usize).init(0),
            .data = AtomicValue(*KeyValueSkipList).init(map),
            .mutable = AtomicValue(bool).init(true),
            .isFlushed = AtomicValue(bool).init(false),
            .sstable = AtomicValue(?*SSTable).init(null),
        };
        return mtable;
    }

    pub fn deinit(self: *Self) void {
        const data = self.data.load(acquire);
        self.init_alloc.destroy(data);
        if (self.sstable.load(acquire)) |sstbl| {
            sstbl.deinit();
            self.init_alloc.destroy(sstbl);
        }
        self.* = undefined;
    }

    pub fn getId(self: Self) u64 {
        return self.id;
    }

    pub fn put(self: *Self, item: KV) !void {
        if (!self.mutable.load(acquire)) {
            return error.MemtableImmutable;
        }

        var data = self.data.load(acquire);
        try data.put(item.key, item.value);
        _ = self.byte_count.fetchAdd(item.len(), release);
    }

    pub fn get(self: Self, key: []const u8) ?KV {
        const value = self.data.load(acquire).get(key) catch |err| {
            print("key {s} {s}\n", .{ key, @errorName(err) });
            return null;
        };
        return KV.init(key, value);
    }

    pub fn count(self: Self) usize {
        return self.data.load(acquire).count();
    }

    pub fn freeze(self: *Self) void {
        self.mutable.store(false, release);
    }

    pub fn frozen(self: Self) bool {
        return !self.mutable.load(acquire);
    }

    pub fn flushed(self: Self) bool {
        return self.isFlushed.load(acquire);
    }

    pub fn size(self: Self) usize {
        return self.byte_count.load(acquire);
    }

    const MemtableIterator = struct {
        alloc: Allocator,
        iter: Iterator(KeyValueSkipList.Item),

        pub fn deinit(ctx: *anyopaque) void {
            const self: *MemtableIterator = @ptrCast(@alignCast(ctx));
            self.iter.deinit();
            self.alloc.destroy(self);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const self: *MemtableIterator = @ptrCast(@alignCast(ctx));
            if (self.iter.next()) |nxt| {
                return KV.init(nxt.key, nxt.value);
            }
            return null;
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        const it = try alloc.create(MemtableIterator);
        it.* = .{ .alloc = alloc, .iter = try self.data.load(acquire).iterator(alloc) };
        return Iterator(KV).init(it, MemtableIterator.next, MemtableIterator.deinit);
    }

    pub fn flush(self: *Self) !void {
        if (self.count() == 0) {
            return;
        }

        mtx.lock();
        defer mtx.unlock();

        if (self.isFlushed.load(acquire)) {
            return error.AlreadyFlushed;
        }

        self.freeze();

        var sstable = try SSTable.init(self.init_alloc, self.getId(), self.opts);
        errdefer self.init_alloc.destroy(sstable);
        errdefer sstable.deinit();

        var it = try self.iterator(self.init_alloc);
        defer it.deinit();

        while (it.next()) |nxt| {
            _ = sstable.write(nxt) catch |err| switch (err) {
                error.DuplicateError => continue,
                else => {
                    print(
                        "memtable not able to write to sstable for key {s}: {s}\n",
                        .{ nxt.key, @errorName(err) },
                    );
                    return err;
                },
            };
        }

        try sstable.flush();

        self.sstable.store(sstable, seq_cst);

        self.isFlushed.store(true, release);
    }
};

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var mtable = try Memtable.init(alloc, 0, options.defaultOpts());
    defer alloc.destroy(mtable);
    defer mtable.deinit();

    // when
    const kv = KV.init("__key__", "__value__");

    try mtable.put(kv);
    const actual = mtable.get(kv.key);

    // then
    try testing.expectEqualStrings(kv.value, actual.?.value);

    mtable.flush() catch |err| {
        print("{s}\n", .{@errorName(err)});
    };
}

test "Memtable thread safety" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Thread = std.Thread;

    // given
    var mtable = try Memtable.init(alloc, 0, options.defaultOpts());
    defer alloc.destroy(mtable);
    defer mtable.deinit();

    const thread_fn = struct {
        fn run(mt: *Memtable, id: usize) void {
            var i: usize = 0;
            while (i < 100) : (i += 1) {
                const key = std.fmt.allocPrint(alloc, "key_{d}_{d}", .{ id, i }) catch continue;
                defer alloc.free(key);

                const value = std.fmt.allocPrint(alloc, "value_{d}_{d}", .{ id, i }) catch continue;
                defer alloc.free(value);

                const kv = KV.init(key, value);
                mt.put(kv) catch continue;
            }
        }
    }.run;

    var threads = std.ArrayList(Thread).init(alloc);
    defer threads.deinit();

    for (0..4) |i| {
        const thread = Thread.spawn(.{}, thread_fn, .{ mtable, i }) catch @panic("Failed to spawn thread");
        try threads.append(thread);
    }

    for (threads.items) |thread| {
        thread.join();
    }

    try testing.expect(mtable.count() > 0);

    var flush_cnt: AtomicValue(u8) = AtomicValue(u8).init(0);

    // Test concurrent flush attempts
    const flush_fn = struct {
        fn run(mt: *Memtable, cnt: *AtomicValue(u8)) void {
            mt.flush() catch return;
            _ = cnt.fetchAdd(1, .seq_cst);
        }
    }.run;

    threads.clearRetainingCapacity();

    try threads.append(Thread.spawn(.{}, flush_fn, .{ mtable, &flush_cnt }) catch @panic("Failed to spawn thread"));
    try threads.append(Thread.spawn(.{}, flush_fn, .{ mtable, &flush_cnt }) catch @panic("Failed to spawn thread"));
    try threads.append(Thread.spawn(.{}, flush_fn, .{ mtable, &flush_cnt }) catch @panic("Failed to spawn thread"));

    for (threads.items) |thread| {
        thread.join();
    }

    try testing.expect(mtable.flushed());
    try testing.expectEqual(flush_cnt.load(.seq_cst), 1);
}
