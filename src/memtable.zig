const std = @import("std");

const file_utils = @import("file.zig");
const keyvalue = @import("kv.zig");
const iter = @import("iterator.zig");
const options = @import("opts.zig");
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

const KeyValueSkipList = SkipList([]const u8, keyvalue.decode, keyvalue.encode);

const assert = std.debug.assert;
const print = std.debug.print;

var mtx: Mutex = .{};

pub const Memtable = struct {
    init_alloc: Allocator,
    cap: usize,
    id: []const u8,
    opts: Opts,

    byte_count: AtomicValue(usize),
    data: AtomicValue(*KeyValueSkipList),
    mutable: AtomicValue(bool),
    isFlushed: AtomicValue(bool),

    // Memory ordering constants for better readability
    const relaxed = .monotonic;
    const acquire = .acquire;
    const release = .release;
    const seq_cst = .seq_cst;

    const Self = @This();

    pub fn init(alloc: Allocator, id: []const u8, opts: Opts) !*Self {
        const cap = opts.sst_capacity;

        const id_copy = try alloc.alloc(u8, id.len);
        @memcpy(id_copy, id);

        const map = try alloc.create(KeyValueSkipList);
        map.* = try KeyValueSkipList.init(alloc);

        const mtable = try alloc.create(Self);
        mtable.* = .{
            .init_alloc = alloc,
            .cap = cap,
            .id = id_copy,
            .opts = opts,
            .byte_count = AtomicValue(usize).init(0),
            .data = AtomicValue(*KeyValueSkipList).init(map),
            .mutable = AtomicValue(bool).init(true),
            .isFlushed = AtomicValue(bool).init(false),
        };
        return mtable;
    }

    pub fn deinit(self: *Self) void {
        const data = self.data.load(acquire);
        self.init_alloc.destroy(data);
        self.init_alloc.free(self.id);
        self.* = undefined;
    }

    pub fn getId(self: Self) []const u8 {
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
};

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(dir_name);
    defer testDir.dir.deleteTree(dir_name) catch {};

    // given
    var mtable = try Memtable.init(alloc, "0", options.withDataDirOpts(dir_name));
    defer alloc.destroy(mtable);
    defer mtable.deinit();

    // when
    const kv = KV.init("__key__", "__value__");

    try mtable.put(kv);
    const actual = mtable.get(kv.key);

    // then
    try testing.expectEqualStrings(kv.value, actual.?.value);
}
