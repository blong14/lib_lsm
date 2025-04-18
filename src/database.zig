const std = @import("std");

const ba = @import("bump_allocator.zig");
const file = @import("file.zig");
const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const lsm = @import("lib.zig");
const mtbl = @import("memtable.zig");
const opt = @import("opts.zig");
const sst = @import("sstable.zig");

const atomic = std.atomic;
const debug = std.debug;
const math = std.math;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const AtomicValue = atomic.Value;
const Mutex = std.Thread.Mutex;
const Order = math.Order;

const ThreadSafeBumpAllocator = ba.ThreadSafeBumpAllocator;
const Iterator = iter.Iterator;
const MergeIterator = iter.MergeIterator;
const ScanIterator = iter.ScanIterator;
const KV = keyvalue.KV;
const Memtable = mtbl.Memtable;
const Opts = opt.Opts;
const SSTable = sst.SSTable;

const TAG = "[zig]";
var mtx: Mutex = .{};

fn compare(a: KV, b: KV) std.math.Order {
    return mem.order(u8, a.key, b.key);
}

pub const Database = struct {
    alloc: Allocator,
    byte_allocator: ThreadSafeBumpAllocator,
    capacity: usize,
    mtable: AtomicValue(*Memtable),
    opts: Opts,
    mtables: std.ArrayList(*Memtable),

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        var byte_allocator = ThreadSafeBumpAllocator.init(alloc, 1096) catch |err| {
            std.log.err("{s} unable to init bump allocator {s}", .{ TAG, @errorName(err) });
            return err;
        };
        errdefer byte_allocator.deinit();

        var mtable = Memtable.init(alloc, 0, opts) catch |err| {
            std.log.err("{s} unable to init memtable {s}", .{ TAG, @errorName(err) });
            return err;
        };
        errdefer mtable.deinit();

        const mtables = std.ArrayList(*Memtable).init(alloc);
        errdefer mtables.deinit();

        const capacity = opts.sst_capacity;

        const db = alloc.create(Self) catch |err| {
            std.log.err("{s} unable to allocate db {s}", .{ TAG, @errorName(err) });
            return err;
        };

        db.* = .{
            .alloc = alloc,
            .byte_allocator = byte_allocator,
            .capacity = capacity,
            .mtable = AtomicValue(*Memtable).init(mtable),
            .mtables = mtables,
            .opts = opts,
        };
        return db;
    }

    pub fn deinit(self: *Self) void {
        mtx.lock();
        defer mtx.unlock();

        for (self.mtables.items) |mtable| {
            mtable.deinit();
            self.alloc.destroy(mtable);
        }
        self.mtables.deinit();

        var mtable = self.mtable.load(.seq_cst);
        mtable.deinit();
        self.alloc.destroy(mtable);

        self.byte_allocator.printStats();
        self.byte_allocator.deinit();

        self.* = undefined;
    }

    pub fn open(self: *Self) !void {
        std.log.info("{s} database opening...", .{TAG});

        var data_dir = try std.fs.openDirAbsolute(self.opts.data_dir, .{ .iterate = true });
        defer data_dir.close();

        var dir_iter = try data_dir.walk(self.alloc);
        defer dir_iter.deinit();

        var count: u64 = 0;
        while (try dir_iter.next()) |f| {
            const nxt_file = try std.fmt.allocPrint(
                self.alloc,
                "{s}/{s}",
                .{ self.opts.data_dir, f.basename },
            );
            errdefer self.alloc.free(nxt_file);

            const data_file = file.open(nxt_file) catch |err| switch (err) {
                error.IsDir => {
                    self.alloc.free(nxt_file);
                    continue;
                },
                else => return err,
            };

            var sstable = try SSTable.init(self.alloc, count, self.opts);
            defer self.alloc.destroy(sstable);
            defer sstable.deinit();

            try sstable.open(data_file);

            var siter = try sstable.iterator(self.alloc);
            defer siter.deinit();

            while (siter.next()) |nxt| {
                try self.write(nxt.key, nxt.value);
            }

            const mtable = self.mtable.load(.seq_cst);
            mtable.isFlushed.store(true, .seq_cst);
            try self.freeze(mtable);

            count += 1;

            self.alloc.free(nxt_file);
        }

        std.log.info("{s} database opened @ {s} w/ {d} warm tables", .{ TAG, self.opts.data_dir, self.mtables.items.len });
    }

    pub fn read(self: Self, key: []const u8) !KV {
        if (self.mtable.load(.seq_cst).get(key)) |value| {
            return value;
        }

        mtx.lock();
        defer mtx.unlock();

        if (self.mtables.items.len > 0) {
            var idx: usize = self.mtables.items.len - 1;
            while (idx > 0) {
                var table = self.mtables.items[idx];
                if (table.get(key)) |value| {
                    return value;
                }
                idx -= 1;
            }
            return error.NotFound;
        } else {
            return error.NotFound;
        }
    }

    const MergeIteratorWrapper = struct {
        alloc: Allocator,
        merger: *MergeIterator(KV, compare),
        iter: Iterator(KV),

        pub fn deinit(ctx: *anyopaque) void {
            var wrapper: *@This() = @ptrCast(@alignCast(ctx));
            wrapper.iter.deinit();
            wrapper.alloc.destroy(wrapper.merger);
            wrapper.alloc.destroy(wrapper);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            var wrapper: *@This() = @ptrCast(@alignCast(ctx));
            return wrapper.iter.next();
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        var merger = try alloc.create(MergeIterator(KV, compare));
        merger.* = try MergeIterator(KV, compare).init(alloc);

        const hot_table = self.mtable.load(.seq_cst);
        const hot_iter = try hot_table.iterator(alloc);
        try merger.add(hot_iter);

        var warm_tables: std.ArrayList(*Memtable) = undefined;
        {
            mtx.lock();
            defer mtx.unlock();
            warm_tables = try self.mtables.clone();
        }
        defer warm_tables.deinit();

        for (warm_tables.items) |mtable| {
            const warm_iter = try mtable.iterator(alloc);
            try merger.add(warm_iter);
        }

        const wrapper = try alloc.create(MergeIteratorWrapper);
        errdefer alloc.destroy(wrapper);

        wrapper.* = .{
            .alloc = alloc,
            .iter = merger.iterator(),
            .merger = merger,
        };

        return Iterator(KV).init(wrapper, MergeIteratorWrapper.next, MergeIteratorWrapper.deinit);
    }

    const ScanWrapper = struct {
        alloc: Allocator,
        scanner: *ScanIterator(KV, compare),
        iterator: Iterator(KV),

        const Wrapper = @This();

        pub fn deinit(ctx: *anyopaque) void {
            const sw: *Wrapper = @ptrCast(@alignCast(ctx));
            sw.iterator.deinit();
            sw.alloc.destroy(sw.scanner);
            sw.alloc.destroy(sw);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const sw: *Wrapper = @ptrCast(@alignCast(ctx));
            return sw.iterator.next();
        }
    };

    /// Creates a scan iterator that returns items with keys
    /// greater than start_key and less than or equal to end_key
    pub fn scan(
        self: *Self,
        alloc: Allocator,
        start_key: []const u8,
        end_key: []const u8,
    ) !Iterator(KV) {
        const start = KV.init(start_key, "");
        const end = KV.init(end_key, "");

        const base_iter = try self.iterator(alloc);

        const si = try alloc.create(ScanIterator(KV, compare));
        si.* = ScanIterator(KV, compare).init(base_iter, start, end);

        const wrapper = try alloc.create(ScanWrapper);
        errdefer alloc.destroy(wrapper);

        wrapper.* = .{
            .alloc = alloc,
            .iterator = si.iterator(),
            .scanner = si,
        };

        return Iterator(KV).init(wrapper, ScanWrapper.next, ScanWrapper.deinit);
    }

    pub fn write(self: *Self, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0 or !std.unicode.utf8ValidateSlice(key)) {
            return error.WriteError;
        }

        const item = KV.init(key, value);

        var mtable = self.mtable.load(.seq_cst);
        if ((mtable.size() + item.len()) >= self.capacity) {
            try self.freeze(mtable);
        }

        mtable = self.mtable.load(.seq_cst);
        try mtable.put(item);
    }

    pub fn flush(self: *Self) !void {
        mtx.lock();
        defer mtx.unlock();

        const hot_table = self.mtable.load(.seq_cst);
        defer self.alloc.destroy(hot_table);

        hot_table.freeze();
        if (!hot_table.flushed()) {
            try hot_table.flush();
        }

        const current_id = hot_table.getId();
        const nxt_table = try Memtable.init(self.alloc, current_id + 1, self.opts);

        self.mtable.store(nxt_table, .seq_cst);

        for (self.mtables.items) |mtable| {
            if (!mtable.flushed()) {
                try mtable.flush();
            }
            mtable.deinit();
            self.alloc.destroy(mtable);
        }

        self.mtables.clearRetainingCapacity();
    }

    fn freeze(self: *Self, mtable: *Memtable) !void {
        mtx.lock();
        defer mtx.unlock();

        if (mtable.frozen()) {
            return;
        }

        mtable.freeze();

        const current_id = mtable.getId();
        const nxt_table = try Memtable.init(self.alloc, current_id + 1, self.opts);

        try self.mtables.append(mtable);

        self.mtable.store(nxt_table, .seq_cst);
    }
};

test Database {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer db.deinit();

    // given
    const key = "__key__";
    const value = "__value__";

    // when
    try db.write(key, value);

    // then
    const actual = try db.read(key);
    try testing.expectEqualStrings(value, actual.value);
}

test "basic functionality with many items" {
    const alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(dir_name);
    defer testDir.dir.deleteTree(dir_name) catch {};

    const db = try lsm.databaseFromOpts(alloc, opt.withDataDirOpts(dir_name));
    defer alloc.destroy(db);
    defer db.deinit();

    // given
    const kvs = [5]KV{
        KV.init("__key_c__", "__value_c__"),
        KV.init("__key_b__", "__value_b__"),
        KV.init("__key_d__", "__value_d__"),
        KV.init("__key_a__", "__value_a__"),
        KV.init("__key_e__", "__value_e__"),
    };

    // when
    for (kvs) |kv| {
        try db.write(kv.key, kv.value);
    }

    // then
    for (kvs) |kv| {
        const actual = try db.read(kv.key);
        try testing.expectEqualStrings(kv.value, actual.value);
    }

    // then
    var it = try db.iterator(alloc);
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |_| {
        count += 1;
    }

    try testing.expectEqual(kvs.len, count);

    var items = std.ArrayList(KV).init(alloc);
    defer items.deinit();

    var scan_iter = try db.scan(alloc, "__key_b__", "__key_e__");
    defer scan_iter.deinit();

    while (scan_iter.next()) |nxt| {
        try items.append(nxt);
    }

    try testing.expectEqual(3, items.items.len);
}
