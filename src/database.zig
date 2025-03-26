const std = @import("std");

const ba = @import("bump_allocator.zig");
const file = @import("file.zig");
const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const lsm = @import("lib");
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
const KV = keyvalue.KV;
const Memtable = mtbl.Memtable;
const Opts = opt.Opts;
const SSTable = sst.SSTable;

var mtx: Mutex = .{};

pub const Database = struct {
    alloc: Allocator,
    byte_allocator: ThreadSafeBumpAllocator,
    capacity: usize,
    mtable: AtomicValue(*Memtable),
    opts: Opts,
    mtables: std.ArrayList(*Memtable),

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        const byte_allocator = try ThreadSafeBumpAllocator.init(alloc, std.mem.page_size);
        const mtable = try Memtable.init(alloc, 0, opts);
        const mtables = std.ArrayList(*Memtable).init(alloc);
        const capacity = opts.sst_capacity;

        const db = try alloc.create(Self);
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

        self.byte_allocator.deinit();

        self.* = undefined;
    }

    pub fn open(self: *Self) !void {
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
            try self.freeze(mtable);

            count += 1;

            self.alloc.free(nxt_file);
        }

        debug.print("database opened @ {s} w/ {d} warm tables\n", .{ self.opts.data_dir, self.mtables.items.len });
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

    const MergeIterator = struct {
        alloc: Allocator,
        mtbls: std.ArrayList(*Iterator(KV)),
        queue: std.PriorityQueue(KV, void, lessThan),

        fn lessThan(context: void, a: KV, b: KV) Order {
            _ = context;
            return mem.order(u8, a.key, b.key);
        }

        pub fn init(dalloc: Allocator, db: *const Database) !MergeIterator {
            const hot_table = db.mtable.load(.seq_cst);

            var warm_tables: std.ArrayList(*Memtable) = undefined;
            defer warm_tables.deinit();
            {
                mtx.lock();
                defer mtx.unlock();

                warm_tables = try db.mtables.clone();
            }

            var iters = std.ArrayList(*Iterator(KV)).init(dalloc);

            const it = try dalloc.create(Iterator(KV));
            it.* = try hot_table.iterator(dalloc);
            try iters.append(it);

            for (warm_tables.items) |mtable| {
                const mit = try dalloc.create(Iterator(KV));
                mit.* = try mtable.iterator(dalloc);
                try iters.append(mit);
            }

            var queue = std.PriorityQueue(KV, void, lessThan).init(dalloc, {});
            try queue.ensureTotalCapacity(std.mem.page_size);

            return .{
                .alloc = dalloc,
                .mtbls = iters,
                .queue = queue,
            };
        }

        pub fn deinit(ctx: *anyopaque) void {
            var mi: *MergeIterator = @ptrCast(@alignCast(ctx));
            for (mi.mtbls.items) |item| {
                item.deinit();
                mi.alloc.destroy(item);
            }
            mi.mtbls.deinit();
            mi.queue.deinit();
            mi.alloc.destroy(mi);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const mi: *MergeIterator = @ptrCast(@alignCast(ctx));
            for (mi.mtbls.items) |table_iter| {
                if (table_iter.next()) |kv| {
                    mi.queue.add(kv) catch |err| {
                        debug.print(
                            "merge iter next failure: {s}\n",
                            .{@errorName(err)},
                        );
                        return null;
                    };
                }
            }

            if (mi.queue.count() == 0) {
                return null;
            }

            const nxt = mi.queue.remove();

            return nxt;
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        const it = try alloc.create(MergeIterator);
        it.* = try MergeIterator.init(alloc, self);
        return Iterator(KV).init(it, MergeIterator.next, MergeIterator.deinit);
    }

    const ScanIterator = struct {
        alloc: Allocator,
        it: Iterator(KV),
        start_key: []const u8,
        end_key: []const u8,

        pub fn init(alloc: Allocator, start_key: []const u8, end_key: []const u8, it: Iterator(KV)) ScanIterator {
            return .{
                .alloc = alloc,
                .it = it,
                .start_key = start_key,
                .end_key = end_key,
            };
        }

        pub fn deinit(ctx: *anyopaque) void {
            var self: *ScanIterator = @ptrCast(@alignCast(ctx));
            self.it.deinit();
            self.alloc.destroy(self);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            var self: *ScanIterator = @ptrCast(@alignCast(ctx));
            while (self.it.next()) |nxt| {
                switch (mem.order(u8, nxt.key, self.start_key)) {
                    .lt => continue,
                    .eq => return nxt,
                    .gt => {
                        switch (mem.order(u8, nxt.key, self.end_key)) {
                            .lt => return nxt,
                            .eq => return nxt,
                            .gt => return null,
                        }
                    },
                }
            }
            return null;
        }
    };

    pub fn scan(self: *Self, alloc: Allocator, start_key: []const u8, end_key: []const u8) !Iterator(KV) {
        const mit = try self.iterator(alloc);
        const it = try alloc.create(ScanIterator);
        it.* = ScanIterator.init(alloc, start_key, end_key, mit);
        return Iterator(KV).init(it, ScanIterator.next, ScanIterator.deinit);
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
        hot_table.freeze();
        try hot_table.flush();

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

    var scan_iter = try db.scan(alloc, "__key_b__", "__key_d__");
    defer scan_iter.deinit();

    while (scan_iter.next()) |nxt| {
        try items.append(nxt);
    }

    try testing.expectEqual(3, items.items.len);
}
