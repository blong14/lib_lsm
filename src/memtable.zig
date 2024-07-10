const std = @import("std");

const file_utils = @import("file.zig");
const keyvalue = @import("kv.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tbm = @import("tablemap.zig");

const Allocator = std.mem.Allocator;

const KV = keyvalue.KV;
const Opts = options.Opts;
const SSTable = sst.SSTable;
const TableMap = tbm.TableMap;

pub const Memtable = struct {
    alloc: Allocator,
    cap: usize,
    hash_map: *TableMap(KV),
    id: u64,
    opts: Opts,
    mutable: bool,
    sstable: *SSTable,

    const Self = @This();

    const Error = error{
        Full,
    };

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity / @sizeOf(KV);
        const map = try TableMap(KV).init(alloc);

        const mtable = try alloc.create(Self);
        mtable.* = .{
            .alloc = alloc,
            .cap = cap,
            .hash_map = map,
            .id = id,
            .mutable = true,
            .opts = opts,
            .sstable = undefined,
        };
        return mtable;
    }

    pub fn deinit(self: *Self) void {
        self.hash_map.deinit();
        self.alloc.destroy(self.hash_map);
        if (self.sstable != undefined) {
            self.sstable.deinit();
            self.alloc.destroy(self.sstable);
        }
        self.* = undefined;
    }

    pub fn getId(self: Self) u64 {
        return self.id;
    }

    pub fn put(self: *Self, key: u64, value: *const KV) !void {
        if (!self.mutable) {
            return error.Full;
        }
        try self.hash_map.put(key, value.*);
    }

    pub fn get(self: Self, key: u64) ?KV {
        return self.hash_map.get(key) catch return null;
    }

    pub fn count(self: Self) usize {
        return self.hash_map.count();
    }

    pub fn scan(self: *Self, start: u64, end: u64, out: *std.ArrayList(KV)) !void {
        var scnr = try self.hash_map.scanner(start, end);
        while (scnr.hasNext()) {
            const entry = try scnr.next();
            try out.append(entry.?);
        }
    }

    pub const Iterator = struct {
        hash_iter: TableMap(KV).Iterator,
        k: u64,
        v: KV,

        pub fn key(it: Iterator) u64 {
            return it.k;
        }

        pub fn value(it: Iterator) KV {
            return it.v;
        }

        pub fn next(it: *Iterator) bool {
            if (it.hash_iter.next()) {
                it.*.k = it.hash_iter.key();
                it.*.v = it.hash_iter.value();
                return true;
            }
            return false;
        }

        /// Reset the iterator to the initial index
        pub fn reset(it: *Iterator) void {
            it.hash_iter.reset();
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !*Iterator {
        const hiter = self.hash_map.iterator(0);
        const iter = try alloc.create(Iterator);
        iter.* = .{
            .hash_iter = hiter,
            .k = 0,
            .v = undefined,
        };
        return iter;
    }

    pub fn flush(self: *Self) !void {
        if (!self.mutable or self.count() == 0) {
            return;
        }

        // TODO: Use a fixed buffer allocator here
        const filename = try std.fmt.allocPrint(self.alloc, "{s}/{d}.dat", .{ self.opts.data_dir, self.getId() });
        defer self.alloc.free(filename);

        const file = try file_utils.openWithCapacity(filename, self.opts.sst_capacity);
        var sstable = try SSTable.init(self.alloc, self.getId(), self.opts);
        try sstable.connect(file);

        var iter = try self.iterator(self.alloc);
        defer self.alloc.destroy(iter);
        while (iter.next()) {
            _ = sstable.write(iter.value()) catch continue;
        }

        sstable.flush() catch {};

        self.sstable = sstable;
        self.mutable = false;
    }
};

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var mtable = try Memtable.init(alloc, 0, options.defaultOpts());
    defer alloc.destroy(mtable);
    defer mtable.deinit();
    defer mtable.flush() catch {};

    // when
    var kv: KV = .{
        .key = "__key__",
        .value = "__value__",
        .hash = 1,
    };
    try mtable.put(1, &kv);
    const actual = mtable.get(1);

    // then
    try testing.expectEqualStrings("__value__", actual.?.value);

    // when
    var next_actual: KV = undefined;
    var iter = try mtable.iterator(alloc);
    defer alloc.destroy(iter);
    if (iter.next()) {
        next_actual = iter.value();
    }

    // then
    try testing.expectEqualStrings("__value__", next_actual.value);
}
