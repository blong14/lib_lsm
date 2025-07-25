const std = @import("std");

const ba = @import("bump_allocator.zig");
const file = @import("file.zig");
const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const lsm = @import("lib.zig");
const mtbl = @import("memtable.zig");
const opt = @import("opts.zig");
const sst = @import("sstable.zig");
const tm = @import("tablemap.zig");

const atomic = std.atomic;
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
const SSTableStore = sst.SSTableStore;

var mtx: Mutex = .{};

/// Events that can trigger agent decisions
pub const DatabaseEvent = union(enum) {
    write_completed: struct { bytes: u64 },
    read_completed: struct { bytes: u64 },
    memtable_frozen: struct { id: []const u8 },
    sstable_created: struct { level: usize, id: []const u8 },
    compaction_completed: struct { level: usize, duration_ns: u64 },
    system_idle: struct { duration_ms: u64 },
};

/// Actions the agent can decide to take
pub const AgentAction = union(enum) {
    flush_memtable: struct { id: []const u8 },
    compact_level: struct { level: usize },
    no_action: void,
};

/// Configurable policy parameters
pub const AgentPolicy = struct {
    max_level0_files: u32 = 16,
    max_unflushed_memtables: usize = 4,
    min_write_cooldown_ms: u64 = 500,
    compaction_level_threshold: f64 = 0.8,
    idle_compact_threshold_ms: u64 = 5000,
};

pub const DatabaseAgent = struct {
    alloc: Allocator,
    db: *Database,
    policy: AgentPolicy,

    action_mutex: std.Thread.Mutex,
    action_queue: std.fifo.LinearFifo(AgentAction, .Dynamic),

    mutex: std.Thread.Mutex,
    event_queue: std.fifo.LinearFifo(DatabaseEvent, .Dynamic),

    thread: ?std.Thread = null,
    running: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(alloc: Allocator, db: *Database, policy: AgentPolicy) !*Self {
        const agent = try alloc.create(Self);
        agent.* = .{
            .alloc = alloc,
            .db = db,
            .policy = policy,
            .event_queue = std.fifo.LinearFifo(DatabaseEvent, .Dynamic).init(alloc),
            .action_queue = std.fifo.LinearFifo(AgentAction, .Dynamic).init(alloc),
            .running = std.atomic.Value(bool).init(false),
            .action_mutex = std.Thread.Mutex{},
            .mutex = std.Thread.Mutex{},
        };
        return agent;
    }

    pub fn deinit(self: *Self) void {
        self.event_queue.deinit();
        self.action_queue.deinit();
        self.alloc.destroy(self);
    }

    pub fn start(self: *Self) !void {
        if (self.isRunning()) return;

        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, Self.run, .{self});
    }

    pub fn stop(self: *Self) void {
        if (!self.isRunning()) return;

        self.running.store(false, .release);
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    pub fn isRunning(self: Self) bool {
        return self.running.load(.acquire);
    }

    /// Runs the database agent targeting 120 FPS rate.
    /// This function implements a game loop pattern with:
    /// 1. Event processing, state evaluation, and action processing every frame
    /// 2. Sleep management to maintain consistent frame rate when possible
    /// 3. Time tracking to monitor performance and detect frame rate drops
    fn run(self: *Self) void {
        // For 120 FPS, each frame should take approximately 8.33ms
        const target_frame_time_ns: i128 = @divTrunc(std.time.ns_per_s, 120);

        var previous = std.time.nanoTimestamp();

        while (self.isRunning()) {
            const current = std.time.nanoTimestamp();
            previous = current;

            self.processEvents();
            self.evaluateState();
            self.processActions();

            // Calculate how long this frame took
            const frame_time = std.time.nanoTimestamp() - current;

            // Sleep if we're ahead of schedule to maintain 120 FPS
            if (frame_time < target_frame_time_ns) {
                const sleep_time_ns = target_frame_time_ns - frame_time;
                std.time.sleep(@intCast(sleep_time_ns));
            } else {
                // We're running behind schedule - log a warning if significantly behind
                const frame_time_ms = @divFloor(frame_time, std.time.ns_per_ms);
                if (frame_time > target_frame_time_ns * 2) {
                    std.log.warn("frame time ({d:.2}ms) exceeded target ({d:.2}ms) by more than 2x", .{
                        frame_time_ms,
                        @divFloor(target_frame_time_ns, std.time.ns_per_ms),
                    });
                }
            }
        }
    }

    fn queueAction(self: *Self, action: AgentAction) void {
        self.action_mutex.lock();
        defer self.action_mutex.unlock();

        self.action_queue.writeItem(action) catch |err| {
            std.log.err("failed to queue action: {s}", .{@errorName(err)});
        };
    }

    fn processActions(self: *Self) void {
        self.action_mutex.lock();
        defer self.action_mutex.unlock();

        if (self.action_queue.readItem()) |action| {
            switch (action) {
                .flush_memtable => |_| {
                    std.log.debug("flush_memtable", .{});

                    self.db.xflush(self.alloc) catch |err| switch (err) {
                        error.NothingToFlush => return,
                        else => {
                            std.log.err(
                                "agent not able to flush memtable {s}",
                                .{@errorName(err)},
                            );
                            return;
                        },
                    };
                },
                .compact_level => |data| {
                    std.log.debug("compact_level {d}", .{data.level});

                    self.db.sstables.compact(data.level) catch |err| {
                        std.log.err(
                            "agent not able to compact level {d} {s}",
                            .{ data.level, @errorName(err) },
                        );
                        return;
                    };
                },
                .no_action => return,
            }
        }
    }

    pub fn submitEvent(self: *Self, event: DatabaseEvent) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.event_queue.writeItem(event) catch |err| {
            std.log.err("failed to submit event: {s}", .{@errorName(err)});
        };
    }

    fn processEvents(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.event_queue.readItem()) |event| {
            switch (event) {
                .write_completed => |data| {
                    // std.log.debug("{s} {s} write_completed {d}", .{ TAG, event, data.bytes });

                    _ = data;
                },
                .read_completed => |data| {
                    std.log.debug("read_completed", .{});

                    // Update read statistics if needed
                    _ = data;
                },
                .memtable_frozen => |data| {
                    std.log.debug("memtable_frozen {s}", .{data.id});

                    self.considerFlushingMemtable(data.id);
                },
                .sstable_created => |data| {
                    std.log.debug("sstable_created", .{});

                    self.considerCompactingLevel(data.level);
                },
                .compaction_completed => |data| {
                    std.log.debug("compaction_completed", .{});

                    // Update compaction statistics
                    _ = data;
                },
                .system_idle => |data| {
                    std.log.debug("system_idle", .{});

                    // Consider background maintenance during idle time
                    self.considerIdleCompaction(data.duration_ms);
                },
            }
        }
    }

    fn evaluateState(self: *Self) void {
        // Periodically evaluate the overall state of the database
        // and make decisions based on the current statistics

        // Check level 0 file count
        const level0_files = self.db.sstables.stats.getFilesCount(0);
        if (level0_files > self.policy.max_level0_files) {
            std.log.debug(
                "level 0 files ({d}) exceeds policy {d}",
                .{ level0_files, self.policy.max_level0_files },
            );
            self.queueAction(.{ .compact_level = .{ .level = 0 } });
        }

        // Check other levels based on size ratios
        for (1..self.db.sstables.num_levels) |level| {
            const level_stats = self.db.sstables.getLevelStats(level) orelse continue;
            const next_level_stats = self.db.sstables.getLevelStats(level + 1) orelse continue;

            // If this level is getting too full compared to the next level
            if (level_stats.files_count > 0 and next_level_stats.files_count > 0) {
                const ratio = @as(f64, @floatFromInt(level_stats.files_count)) /
                    @as(f64, @floatFromInt(next_level_stats.files_count));

                if (ratio > self.policy.compaction_level_threshold) {
                    std.log.debug(
                        "level {d} file ratio ({d}) exceeds policy {d}",
                        .{ level, ratio, self.policy.compaction_level_threshold },
                    );
                    self.queueAction(.{ .compact_level = .{ .level = level } });
                }
            }
        }
    }

    fn considerFlushingMemtable(self: *Self, id: []const u8) void {
        // Simple policy: if we have too many unflushed memtables, flush this one
        const count = self.db.memtableCount();
        if (count >= self.policy.max_unflushed_memtables) {
            std.log.debug(
                "memtable count ({d}) exceeds policy {d}",
                .{ count, self.policy.max_unflushed_memtables },
            );
            self.queueAction(.{ .flush_memtable = .{ .id = id } });
        }
    }

    fn considerCompactingLevel(self: *Self, level: usize) void {
        // Check if this level needs compaction based on file count
        const files_count = self.db.sstables.stats.getFilesCount(level);
        const max_files = self.policy.max_level0_files * std.math.pow(u32, 10, @intCast(level));

        if (files_count > max_files) {
            std.log.debug(
                "level {d} file count ({d}) exceeds max files {d}",
                .{ level, files_count, max_files },
            );
            self.queueAction(.{ .compact_level = .{ .level = level } });
        }
    }

    fn considerIdleCompaction(self: *Self, idle_ms: u64) void {
        if (idle_ms < self.policy.idle_compact_threshold_ms) return;

        // Find a level that hasn't been compacted in a while
        var oldest_level: usize = 0;
        var oldest_time: i64 = std.time.timestamp();

        for (0..self.db.sstables.num_levels) |level| {
            const last_compaction = self.db.sstables.stats.getLastCompactionTime(level);
            if (last_compaction > 0 and last_compaction < oldest_time) {
                oldest_time = last_compaction;
                oldest_level = level;
            }
        }

        // If we found a level that hasn't been compacted in a while
        const now = std.time.timestamp();
        if (now - oldest_time > @as(i64, @intCast(self.policy.idle_compact_threshold_ms / 1000))) {
            std.log.debug(
                "level {d} compaction time exceeds ({d}) policy {d}",
                .{ oldest_level, now - oldest_time, @as(i64, @intCast(self.policy.idle_compact_threshold_ms / 1000)) },
            );
            self.queueAction(.{ .compact_level = .{ .level = oldest_level } });
        }
    }
};

pub const Database = struct {
    agent: *DatabaseAgent,
    capacity: usize,
    mtable: AtomicValue(*Memtable),
    opts: Opts,
    mtables: std.ArrayList(*Memtable),
    sstables: *SSTableStore,

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !*Self {
        var new_id_buf: [64]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.nanoTimestamp()});

        const mtable = Memtable.init(alloc, new_id, opts) catch |err| {
            std.log.err("unable to init memtable {s}", .{@errorName(err)});
            return err;
        };
        errdefer mtable.deinit();

        const mtables = std.ArrayList(*Memtable).init(alloc);
        errdefer mtables.deinit();

        const sstables = try alloc.create(SSTableStore);
        sstables.* = try SSTableStore.init(alloc, opts);

        const capacity = opts.sst_capacity;

        const db = alloc.create(Self) catch |err| {
            std.log.err("unable to allocate db {s}", .{@errorName(err)});
            return err;
        };

        const agent = try DatabaseAgent.init(alloc, db, .{});

        db.* = .{
            .agent = agent,
            .capacity = capacity,
            .mtable = AtomicValue(*Memtable).init(mtable),
            .mtables = mtables,
            .opts = opts,
            .sstables = sstables,
        };

        try db.agent.start();

        return db;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        mtx.lock();
        defer mtx.unlock();

        self.agent.stop();
        self.agent.deinit();

        for (self.mtables.items) |mtable| {
            mtable.deinit();
            alloc.destroy(mtable);
        }
        self.mtables.deinit();

        var mtable = self.mtable.load(.seq_cst);
        mtable.deinit();
        alloc.destroy(mtable);

        self.sstables.deinit(alloc);
        alloc.destroy(self.sstables);

        self.* = undefined;
    }

    pub fn open(self: *Self, alloc: Allocator) !void {
        self.sstables.open(alloc) catch |err| {
            std.log.err("not able to open sstables data directory {s}", .{@errorName(err)});
            return err;
        };

        for (0..self.sstables.num_levels) |lvl| {
            for (self.sstables.get(lvl)) |sstable| {
                var siter = try sstable.iterator(alloc);
                defer siter.deinit();

                const nxt_table = try Memtable.init(alloc, sstable.id, self.opts);
                while (siter.next()) |nxt| {
                    try nxt_table.put(alloc, nxt);
                }

                nxt_table.isFlushed.store(true, .seq_cst);
                nxt_table.mutable.store(false, .seq_cst);

                try self.mtables.append(nxt_table);
            }
        }

        std.log.info("database opened @ {s} w/ {d} warm tables", .{ self.opts.data_dir, self.mtables.items.len });
        std.log.info("current hot table key count {d}", .{self.mtable.load(.seq_cst).count()});
    }

    pub fn read(self: *Self, alloc: Allocator, key: []const u8) !KV {
        const hot_table = self.mtable.load(.seq_cst);
        if (hot_table.get(alloc, key)) |kv| {
            return kv;
        }

        mtx.lock();
        const warm_tables = try self.mtables.clone();
        mtx.unlock();
        defer warm_tables.deinit();

        for (warm_tables.items) |mtable| {
            if (mtable.get(alloc, key)) |kv| {
                return kv;
            }
        }

        const internal_key = try keyvalue.encodeInternalKey(alloc, key, null);
        defer alloc.free(internal_key);

        var kv: KV = undefined;
        try self.sstables.read(key, &kv);

        return kv.clone(alloc);
    }

    const MergeIteratorWrapper = struct {
        alloc: Allocator,
        merger: *MergeIterator(KV, keyvalue.compare),
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
        var merger = try alloc.create(MergeIterator(KV, keyvalue.compare));
        merger.* = try MergeIterator(KV, keyvalue.compare).init(alloc);

        const hot_table = self.mtable.load(.seq_cst);
        if (hot_table.count() > 0) {
            const hot_iter = try hot_table.iterator(alloc);
            try merger.add(hot_iter);
        }

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

        //const siter = try self.sstables.iterator(alloc);
        //try merger.add(siter);

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
        scanner: *ScanIterator(KV, keyvalue.userKeyCompare),
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

        const si = try alloc.create(ScanIterator(KV, keyvalue.userKeyCompare));
        si.* = ScanIterator(KV, keyvalue.userKeyCompare).init(base_iter, start, end);

        const wrapper = try alloc.create(ScanWrapper);
        errdefer alloc.destroy(wrapper);

        wrapper.* = .{
            .alloc = alloc,
            .iterator = si.iterator(),
            .scanner = si,
        };

        return Iterator(KV).init(wrapper, ScanWrapper.next, ScanWrapper.deinit);
    }

    pub fn write(self: *Self, alloc: Allocator, key: []const u8, value: []const u8) anyerror!void {
        if (key.len == 0 or !std.unicode.utf8ValidateSlice(key)) {
            return error.InvalidKeyData;
        }

        const item = KV.init(key, value);

        var mtable = self.mtable.load(.seq_cst);
        if ((mtable.size() + item.len()) >= self.capacity) {
            mtx.lock();
            defer mtx.unlock();
            try self.freeze(alloc, mtable);
        }

        while (true) {
            mtable = self.mtable.load(.seq_cst);
            mtable.put(alloc, item) catch |err| switch (err) {
                error.MemtableImmutable => continue,
                else => return err,
            };
            break;
        }

        self.agent.submitEvent(.{ .write_completed = .{ .bytes = item.len() } });
    }

    fn memtableCount(self: Self) usize {
        mtx.lock();
        defer mtx.unlock();

        var count: usize = 0;
        for (self.mtables.items) |table| {
            if (!table.flushed()) {
                count += 1;
            }
        }

        return count;
    }

    pub fn flush(self: *Self, alloc: Allocator) void {
        const hot_table = self.mtable.load(.acquire);
        self.freeze(alloc, hot_table) catch |err| {
            std.log.err("db freeze failed {s}", .{@errorName(err)});
            return;
        };
        self.xflush(alloc) catch |err| {
            std.log.err("db flush failed {s}", .{@errorName(err)});
        };
    }

    pub fn xflush(self: *Self, alloc: Allocator) !void {
        mtx.lock();
        defer mtx.unlock();

        if (self.mtables.items.len == 0) {
            std.log.debug("nothing to flush", .{});
            return error.NothingToFlush;
        }

        for (self.mtables.items) |table| {
            if (!table.flushed()) {
                self.sstables.flush(alloc, table) catch |err| {
                    std.log.err(
                        "sstables not able to flush mtable {s} {s}",
                        .{ table.getId(), @errorName(err) },
                    );
                    return error.FlushFailed;
                };
            }
        }
    }

    fn freeze(self: *Self, alloc: Allocator, mtable: *Memtable) !void {
        if (mtable.frozen()) return;

        mtable.freeze();

        var new_id_buf: [64]u8 = undefined;
        const new_id = try std.fmt.bufPrint(&new_id_buf, "{d}", .{std.time.nanoTimestamp()});

        const nxt_table = try Memtable.init(alloc, new_id, self.opts);

        try self.mtables.append(mtable);

        self.mtable.store(nxt_table, .seq_cst);

        self.agent.submitEvent(.{ .memtable_frozen = .{ .id = mtable.getId() } });
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
    defer db.deinit(alloc);

    // given
    const key = "__key__";
    const value = "__value__";

    // when
    try db.write(alloc, key, value);

    // then
    var actual = try db.read(alloc, key);
    defer actual.deinit(alloc); // Make sure to free the memory when done

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
    defer db.deinit(alloc);

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
        try db.write(alloc, kv.key, kv.value);
    }

    // then
    var byte_allocator = try ThreadSafeBumpAllocator.init(alloc, 4096);
    defer byte_allocator.deinit();

    const balloc = byte_allocator.allocator();

    for (kvs) |kv| {
        var actual = try db.read(balloc, kv.key);
        defer actual.deinit(balloc);

        try testing.expectEqualStrings(kv.value, actual.value);
    }

    // then
    var it = try db.iterator(balloc);
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |_| {
        count += 1;
    }

    try testing.expectEqual(kvs.len, count);

    var items = std.ArrayList(KV).init(alloc);
    defer items.deinit();

    var scan_iter = try db.scan(balloc, "__key_b__", "__key_d__");
    defer scan_iter.deinit();

    while (scan_iter.next()) |nxt| {
        try items.append(nxt);
    }

    try testing.expectEqual(3, items.items.len);
}
