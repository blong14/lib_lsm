const std = @import("std");

const file_utils = @import("file.zig");
const keyvalue = @import("kv.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tbm = @import("tablemap.zig");

const Allocator = std.mem.Allocator;
const File = std.fs.File;

const KV = keyvalue.KV;
const Opts = options.Opts;
const SSTable = sst.SSTable;
const TableMap = tbm.TableMap;

const print = std.debug.print;

pub const Memtable = struct {
    const Self = @This();

    const KVTableMap = TableMap([]const u8, *const KV, KV.order);

    const Error = error{
        Full,
    };

    alloc: Allocator,
    byte_count: usize,
    cap: usize,
    data: KVTableMap,
    id: u64,
    opts: Opts,
    mutable: bool,
    sstable: ?*SSTable,

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity;
        const map = try KVTableMap.init(alloc, cap);

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
        if (self.*.sstable) |sstbl| {
            sstbl.deinit();
            self.alloc.destroy(sstbl);
        }
        self.* = undefined;
    }

    pub fn getId(self: Self) u64 {
        return self.id;
    }

    pub fn put(self: *Self, item: *const KV) !void {
        if (!self.mutable) {
            return error.Full;
        }
        try self.data.put(item.key, item);
        self.byte_count += item.len();
    }

    pub fn get(self: Self, key: []const u8) ?*const KV {
        return self.data.get(key) catch return null;
    }

    pub fn count(self: Self) usize {
        return self.data.count();
    }

    pub fn size(self: Self) usize {
        return self.byte_count;
    }

    pub const Iterator = struct {
        data: KVTableMap,
        idx: usize,
        start_idx: usize,
        v: *const KV,

        pub fn value(it: Iterator) *const KV {
            return it.v;
        }

        pub fn prev(it: *Iterator) bool {
            if (it.data.count() == 0) {
                return false;
            }
            const entry = it.data.getEntryByIdx(it.*.idx) catch |err| {
                print(
                    "memtable iter error: {s}\n",
                    .{@errorName(err)},
                );
                return false;
            };
            it.*.v = entry;
            if (it.*.idx == 0) {
                return false;
            }
            it.*.idx -= 1;
            return true;
        }

        pub fn next(it: *Iterator) bool {
            if (it.*.idx >= it.data.count()) {
                return false;
            }
            const entry = it.data.getEntryByIdx(it.*.idx) catch |err| {
                print(
                    "memtable iter error: {s}\n",
                    .{@errorName(err)},
                );
                return false;
            };
            it.*.v = entry;
            it.*.idx += 1;
            return true;
        }

        pub fn reset(it: *Iterator) void {
            it.*.idx = it.start_idx;
        }
    };

    pub fn iterator(self: *Self, start: usize) !*Iterator {
        const iter = try self.alloc.create(Iterator);
        iter.* = .{
            .data = self.data,
            .idx = start,
            .start_idx = start,
            .v = undefined,
        };
        return iter;
    }

    pub fn flush(self: *Self) !void {
        if (!self.mutable or self.count() == 0) {
            return;
        }

        var iter = try self.iterator(0);
        defer self.alloc.destroy(iter);

        var sstable = try SSTable.init(self.alloc, self.getId(), self.opts);
        errdefer self.alloc.destroy(sstable);
        errdefer sstable.deinit();

        while (iter.next()) {
            const kv = iter.value();
            _ = sstable.write(kv) catch |err| {
                print(
                    "memtable not able to write to sstable for key {s}: {s}\n",
                    .{ kv.key, @errorName(err) },
                );
                return err;
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

    try mtable.put(&kv);
    const actual = mtable.get(kv.key);

    // then
    try testing.expectEqualStrings(kv.value, actual.?.value);

    mtable.flush() catch |err| {
        std.debug.print("{s}\n", .{@errorName(err)});
    };
}
