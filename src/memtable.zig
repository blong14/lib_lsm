const std = @import("std");

const ba = @import("bump_allocator.zig");
const keyvalue = @import("kv.zig");
const iter = @import("iterator.zig");
const options = @import("opts.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;

const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const Opts = options.Opts;
const SkipList = skl.SkipList;

const KeyValueSkipList = SkipList(KV, keyvalue.decode, keyvalue.encode);

pub const Memtable = struct {
    init_alloc: Allocator,
    cap: usize,
    id: []const u8,
    opts: Opts,

    byte_count: AtomicValue(usize),
    data: AtomicValue(*KeyValueSkipList),
    mutable: AtomicValue(bool),
    isFlushed: AtomicValue(bool),

    const Self = @This();

    pub fn init(alloc: Allocator, id: []const u8, opts: Opts) !*Self {
        const cap = opts.sst_capacity;

        const id_copy = try alloc.alloc(u8, id.len);
        @memcpy(id_copy, id);

        const map = try alloc.create(KeyValueSkipList);
        map.* = try KeyValueSkipList.init();

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
        const data = self.data.load(.acquire);
        self.init_alloc.destroy(data);
        self.init_alloc.free(self.id);
        self.* = undefined;
    }

    pub fn getId(self: Self) []const u8 {
        return self.id;
    }

    pub fn put(self: *Self, alloc: Allocator, item: KV) !void {
        if (!self.mutable.load(.acquire)) {
            return error.MemtableImmutable;
        }

        if (item.raw_bytes) |raw| {
            var data = self.data.load(.acquire);
            try data.putRaw(try item.internalKey(alloc), raw);

            _ = self.byte_count.fetchAdd(raw.len, .release);
        } else {
            return error.MissingRawData;
        }
    }

    pub fn get(self: Self, alloc: Allocator, key: []const u8) ?KV {
        const data = self.data.load(.acquire);

        var iter_obj = data.iterator(alloc) catch return null;
        defer iter_obj.deinit();

        var latest_kv: ?KV = null;
        var latest_timestamp: i128 = std.math.minInt(i128);

        while (iter_obj.next()) |kv| {
            if (std.mem.eql(u8, kv.userKey(), key)) {
                const timestamp = kv.timestamp;

                if (timestamp > latest_timestamp) {
                    latest_kv = kv;
                    latest_timestamp = timestamp;
                }
            }
        }

        if (latest_kv) |kv| {
            return kv;
        }

        return null;
    }

    pub fn count(self: Self) usize {
        return self.data.load(.acquire).count();
    }

    pub fn freeze(self: *Self) void {
        self.mutable.store(false, .release);
    }

    pub fn frozen(self: Self) bool {
        return !self.mutable.load(.acquire);
    }

    pub fn flushed(self: Self) bool {
        return self.isFlushed.load(.acquire);
    }

    pub fn size(self: Self) usize {
        return self.byte_count.load(.acquire);
    }

    const MemtableIterator = struct {
        alloc: Allocator,
        iter: Iterator(KV),

        pub fn deinit(ctx: *anyopaque) void {
            const self: *MemtableIterator = @ptrCast(@alignCast(ctx));
            self.iter.deinit();
            self.alloc.destroy(self);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const self: *MemtableIterator = @ptrCast(@alignCast(ctx));
            if (self.iter.next()) |nxt| {
                return nxt;
            }
            return null;
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        const it = try alloc.create(MemtableIterator);
        it.* = .{ .alloc = alloc, .iter = try self.data.load(.acquire).iterator(alloc) };
        return Iterator(KV).init(it, MemtableIterator.next, MemtableIterator.deinit);
    }
};

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;
    // given
    var mtable = try Memtable.init(alloc, "0", options.defaultOpts());
    defer alloc.destroy(mtable);
    defer mtable.deinit();

    // when
    var kv = try KV.initOwned(alloc, "__key__", "__value__");
    defer kv.deinit(alloc);

    try mtable.put(alloc, kv);

    var actual = mtable.get(alloc, "__key__");
    defer actual.?.deinit(alloc);

    // then
    try testing.expectEqualStrings(kv.value, actual.?.value);
    // TODO: fix ownership once rust lib is updated
    try testing.expectEqual(keyvalue.KVOwnership.owned, actual.?.ownership);

    const kv2 = KV.init("__key__", "__updated_value__");
    try mtable.put(alloc, kv2);

    var latest = mtable.get(alloc, "__key__");
    defer latest.?.deinit(alloc);

    try testing.expectEqualStrings("__updated_value__", latest.?.value);
    // TODO: fix ownership once rust lib is updated
    try testing.expectEqual(keyvalue.KVOwnership.owned, latest.?.ownership);
}
