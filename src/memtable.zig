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

const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const Opts = options.Opts;
const SkipList = skl.SkipList;
const SSTable = sst.SSTable;

const KeyValueSkipList = SkipList([]const u8, keyvalue.decode, keyvalue.encode);

const assert = std.debug.assert;
const print = std.debug.print;

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
        const data = self.data.load(.seq_cst);
        self.init_alloc.destroy(data);
        if (self.sstable.load(.seq_cst)) |sstbl| {
            sstbl.deinit();
            self.init_alloc.destroy(sstbl);
        }
        self.* = undefined;
    }

    pub fn getId(self: Self) u64 {
        return @atomicLoad(u64, &self.id, .seq_cst);
    }

    pub fn put(self: *Self, item: KV) !void {
        var data = self.data.load(.seq_cst);
        try data.put(item.key, item.value);
        _ = self.byte_count.fetchAdd(item.len(), .seq_cst);
    }

    pub fn get(self: Self, key: []const u8) ?KV {
        const value = self.data.load(.seq_cst).get(key) catch |err| {
            print("key {s} {s}\n", .{ key, @errorName(err) });
            return null;
        };
        return KV.init(key, value);
    }

    pub fn count(self: Self) usize {
        return self.data.load(.seq_cst).count();
    }

    pub fn freeze(self: *Self) void {
        self.mutable.store(false, .seq_cst);
    }

    pub fn frozen(self: Self) bool {
        return !self.mutable.load(.seq_cst);
    }

    pub fn flushed(self: Self) bool {
        return self.isFlushed.load(.seq_cst);
    }

    pub fn size(self: Self) usize {
        return self.byte_count.load(.seq_cst);
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
        it.* = .{ .alloc = alloc, .iter = try self.data.load(.seq_cst).iterator(alloc) };
        return Iterator(KV).init(it, MemtableIterator.next, MemtableIterator.deinit);
    }

    pub fn flush(self: *Self) !void {
        if (self.count() == 0) {
            return;
        }

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

        self.sstable.store(sstable, .seq_cst);
        self.isFlushed.store(true, .seq_cst);
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
