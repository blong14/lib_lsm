const std = @import("std");

const file_utils = @import("file.zig");
const keyvalue = @import("kv.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tbm = @import("tablemap.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;
const File = std.fs.File;

const KV = keyvalue.KV;
const Opts = options.Opts;
const SkipList = skl.SkipList;
const SSTable = sst.SSTable;

const assert = std.debug.assert;
const print = std.debug.print;

pub const Memtable = struct {
    alloc: Allocator,
    cap: usize,
    id: u64,
    opts: Opts,

    byte_count: AtomicValue(usize),
    data: AtomicValue(*SkipList([]const u8, keyvalue.decode, keyvalue.encode)),
    mutable: AtomicValue(bool),
    sstable: AtomicValue(?*SSTable),

    const Self = @This();

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity;

        const map = try alloc.create(SkipList([]const u8, keyvalue.decode, keyvalue.encode));
        map.* = try SkipList([]const u8, keyvalue.decode, keyvalue.encode).init(alloc);

        const mtable = try alloc.create(Self);
        mtable.* = .{
            .alloc = alloc,
            .cap = cap,
            .id = id,
            .opts = opts,
            .byte_count = AtomicValue(usize).init(0),
            .data = AtomicValue(*SkipList([]const u8, keyvalue.decode, keyvalue.encode)).init(map),
            .mutable = AtomicValue(bool).init(true),
            .sstable = AtomicValue(?*SSTable).init(null),
        };
        return mtable;
    }

    pub fn deinit(self: *Self) void {
        const data = self.data.load(.seq_cst);
        self.alloc.destroy(data);
        if (self.sstable.load(.seq_cst)) |sstbl| {
            sstbl.deinit();
            self.alloc.destroy(sstbl);
        }
        self.* = undefined;
    }

    pub fn getId(self: Self) u64 {
        return @atomicLoad(u64, &self.id, .seq_cst);
    }

    pub fn put(self: *Self, item: KV) !void {
        try self.data.load(.seq_cst).put(item.key, item.value);
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

    pub fn size(self: Self) usize {
        return self.byte_count.load(.seq_cst);
    }

    pub const Iterator = struct {
        data: SkipList([]const u8, keyvalue.decode, keyvalue.encode).Iterator,
        v: KV,

        pub fn deinit(it: *Iterator) void {
            it.data.deinit();
            it.* = undefined;
        }

        pub fn value(it: Iterator) KV {
            return KV.init(it.data.key(), it.data.value());
        }

        pub fn next(it: *Iterator) !bool {
            return try it.data.next();
        }
    };

    pub fn iterator(self: *Self) !Iterator {
        return .{
            .data = self.data.load(.seq_cst).iter(),
            .v = undefined,
        };
    }

    pub fn flush(self: *Self) !void {
        if (self.count() == 0) {
            return;
        }

        var sstable = try SSTable.init(self.alloc, self.getId(), self.opts);
        errdefer self.alloc.destroy(sstable);
        errdefer sstable.deinit();

        var iter = self.data.load(.seq_cst).iter();
        defer iter.deinit();

        while (try iter.next()) {
            const kv = KV.init(iter.key(), iter.value());
            _ = sstable.write(kv) catch |err| switch (err) {
                error.DuplicateError => continue,
                else => {
                    print(
                        "memtable not able to write to sstable for key {s}: {s}\n",
                        .{ kv.key, @errorName(err) },
                    );
                    return err;
                },
            };
        }

        try sstable.flush();

        self.sstable.store(sstable, .seq_cst);
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
        std.debug.print("{s}\n", .{@errorName(err)});
    };
}
