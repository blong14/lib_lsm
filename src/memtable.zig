const std = @import("std");

const file_utils = @import("file.zig");
const keyvalue = @import("kv.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tbm = @import("tablemap.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const File = std.fs.File;

const KV = keyvalue.KV;
const Opts = options.Opts;
const SkipList = skl.SkipList;
const SSTable = sst.SSTable;

const print = std.debug.print;

pub const Memtable = struct {
    const Self = @This();

    const Error = error{
        Full,
    };

    alloc: Allocator,
    byte_count: usize,
    cap: usize,
    data: *SkipList([]const u8, keyvalue.decode, keyvalue.encode),
    id: u64,
    opts: Opts,
    mutable: bool,
    sstable: ?*SSTable,

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity;
        const map = try SkipList([]const u8, keyvalue.decode, keyvalue.encode).init(alloc);

        const mtable = try alloc.create(Self);
        mtable.* = .{
            .alloc = alloc,
            .byte_count = 0,
            .cap = cap,
            .data = map,
            .id = id,
            .mutable = true,
            .opts = opts,
            .sstable = null,
        };
        return mtable;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.alloc.destroy(self.data);
        if (self.*.sstable) |sstbl| {
            sstbl.deinit();
            self.alloc.destroy(sstbl);
        }
        self.* = undefined;
    }

    pub fn getId(self: Self) u64 {
        return self.id;
    }

    pub fn put(self: *Self, item: KV) !void {
        if (!self.mutable) {
            return error.Full;
        }
        try self.data.put(item.key, item.value);
        self.byte_count += item.len();
    }

    pub fn get(self: Self, key: []const u8) ?KV {
        const value = self.data.get(key) catch |err| {
            print("key {s} {s}\n", .{ key, @errorName(err) });
            return null;
        };
        return KV.init(key, value);
    }

    pub fn count(self: Self) usize {
        return self.data.count();
    }

    pub fn size(self: Self) usize {
        return self.byte_count;
    }

    pub const Iterator = struct {
        data: SkipList([]const u8, keyvalue.decode, keyvalue.encode).Iterator,
        v: KV,

        pub fn deinit(it: *Iterator) void {
            it.data.deinit();
            it.* = undefined;
        }

        pub fn value(it: Iterator) KV {
            return it.v;
        }

        pub fn next(it: *Iterator) !bool {
            if (try it.data.next()) {
                it.*.v = KV.init(it.data.key(), it.data.value());
                return true;
            } else {
                return false;
            }
        }
    };

    pub fn iterator(self: *Self) !*Iterator {
        const iter = try self.alloc.create(Iterator);
        iter.* = .{
            .data = self.data.iter(self.alloc),
            .v = undefined,
        };
        return iter;
    }

    pub fn flush(self: *Self) !void {
        if (!self.mutable or self.count() == 0) {
            return;
        }

        var sstable = try SSTable.init(self.alloc, self.getId(), self.opts);
        errdefer self.alloc.destroy(sstable);
        errdefer sstable.deinit();

        var iter = self.data.iter(self.alloc);
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

        self.*.sstable = sstable;
        self.*.mutable = false;
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
