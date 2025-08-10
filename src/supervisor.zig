const std = @import("std");

const Database = @import("database.zig").Database;

/// Events that can trigger supervisor decisions
pub const DatabaseEvent = union(enum) {
    write_completed: struct { bytes: u64 },
    read_completed: struct { bytes: u64 },
    memtable_frozen: struct { id: []const u8 },
    sstable_created: struct { level: usize },
    compaction_completed: struct { level: usize, duration_ns: i64 },
    system_idle: struct { duration_ms: u64 },
};

/// Actions the supervisor can decide to take
pub const SupervisorAction = union(enum) {
    freeze_memtable: struct { id: []const u8 },
    flush_memtable: struct { id: []const u8 },
    compact_level: struct { level: usize },
    no_action: void,
};

/// Configurable policy parameters
pub const SupervisorPolicy = struct {
    max_level0_files: u32 = 16,
    max_unflushed_memtables: usize = 4,
    min_write_cooldown_ms: u64 = 500,
    compaction_level_threshold: f64 = 0.8,
    idle_compact_threshold_ms: u64 = 5000,
};

pub const DatabaseSupervisor = struct {
    alloc: std.mem.Allocator,
    db: *anyopaque, // Forward declaration to avoid circular dependency
    policy: SupervisorPolicy,

    action_mutex: std.Thread.Mutex,
    action_queue: std.fifo.LinearFifo(SupervisorAction, .Dynamic),

    event_mutex: std.Thread.Mutex,
    event_queue: std.fifo.LinearFifo(DatabaseEvent, .Dynamic),

    thread: ?std.Thread = null,
    running: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(alloc: std.mem.Allocator, db: *anyopaque, policy: SupervisorPolicy) !*Self {
        const supervisor = try alloc.create(Self);
        supervisor.* = .{
            .alloc = alloc,
            .db = db,
            .policy = policy,
            .event_mutex = std.Thread.Mutex{},
            .event_queue = std.fifo.LinearFifo(DatabaseEvent, .Dynamic).init(alloc),
            .action_mutex = std.Thread.Mutex{},
            .action_queue = std.fifo.LinearFifo(SupervisorAction, .Dynamic).init(alloc),
            .running = std.atomic.Value(bool).init(false),
        };
        return supervisor;
    }

    pub fn deinit(self: *Self) void {
        self.event_queue.deinit();
        self.action_queue.deinit();
        self.* = undefined;
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

    /// Runs the database supervisor targeting 120 FPS rate.
    /// This function implements a game loop pattern with:
    /// 1. Event processing, state evaluation, and action processing every frame
    /// 2. Sleep management to maintain consistent frame rate when possible
    /// 3. Time tracking to monitor performance and detect frame rate drops
    fn run(self: *Self) void {
        // For 120 FPS, each frame should take approximately 8.33ms
        const target_frame_time_ns: i128 = @divTrunc(std.time.ns_per_s, 120);

        while (self.isRunning()) {
            const frame_start = std.time.nanoTimestamp();

            // Check if we should still be running before processing
            if (!self.isRunning()) break;

            self.processEvents();

            if (!self.isRunning()) break;

            self.evaluateState();

            if (!self.isRunning()) break;

            self.processActions();

            const frame_time = std.time.nanoTimestamp() - frame_start;

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

    fn queueAction(self: *Self, action: SupervisorAction) void {
        self.action_mutex.lock();
        defer self.action_mutex.unlock();

        self.action_queue.writeItem(action) catch |err| {
            std.log.err("failed to queue action: {s}", .{@errorName(err)});
        };
    }

    fn processActions(self: *Self) void {
        self.action_mutex.lock();
        defer self.action_mutex.unlock();

        const db: *Database = @ptrCast(@alignCast(self.db));

        if (self.action_queue.readItem()) |action| {
            switch (action) {
                .freeze_memtable => |_| {
                    std.log.debug("freeze_memtable", .{});

                    const mtable = db.mtable.load(.seq_cst);
                    db.freeze(self.alloc, mtable) catch |err| {
                        std.log.err(
                            "supervisor not able to freeze memtable {s}",
                            .{@errorName(err)},
                        );
                        return;
                    };

                    db.xflush(self.alloc) catch |err| switch (err) {
                        error.NothingToFlush => return,
                        else => {
                            std.log.err(
                                "supervisor not able to flush memtable {s}",
                                .{@errorName(err)},
                            );
                            return;
                        },
                    };

                    self.submitEvent(.{ .sstable_created = .{ .level = 0 } });
                },
                .flush_memtable => |_| {
                    std.log.debug("flush_memtable", .{});

                    db.xflush(self.alloc) catch |err| switch (err) {
                        error.NothingToFlush => return,
                        else => {
                            std.log.err(
                                "supervisor not able to flush memtable {s}",
                                .{@errorName(err)},
                            );
                            return;
                        },
                    };

                    self.submitEvent(.{ .sstable_created = .{ .level = 0 } });
                },
                .compact_level => |data| {
                    std.log.debug("compact_level {d}", .{data.level});

                    db.sstables.compact(data.level) catch |err| {
                        std.log.err(
                            "supervisor not able to compact level {d} {s}",
                            .{ data.level, @errorName(err) },
                        );
                        return;
                    };

                    var event: DatabaseEvent = .{
                        .compaction_completed = .{
                            .level = data.level,
                            .duration_ns = 0,
                        },
                    };

                    if (db.sstables.getLevelStats(data.level)) |stats| {
                        event.compaction_completed.duration_ns = stats.last_compaction;
                    }

                    self.submitEvent(event);
                },
                .no_action => return,
            }
        }
    }

    pub fn submitEvent(self: *Self, event: DatabaseEvent) void {
        self.event_mutex.lock();
        defer self.event_mutex.unlock();

        self.event_queue.writeItem(event) catch |err| {
            std.log.err("failed to submit event: {s}", .{@errorName(err)});
        };
    }

    fn processEvents(self: *Self) void {
        self.event_mutex.lock();
        defer self.event_mutex.unlock();

        if (self.event_queue.readItem()) |event| {
            switch (event) {
                .write_completed => |data| {
                    self.considerFreezingMemtable(data.bytes);
                },
                .read_completed => |data| {
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
        const db: *Database = @ptrCast(@alignCast(self.db));

        // Periodically evaluate the overall state of the database
        // and make decisions based on the current statistics

        // Check level 0 file count
        const level0_files = db.sstables.stats.getFilesCount(0);
        if (level0_files > self.policy.max_level0_files) {
            std.log.debug(
                "state evaluated: level 0 files ({d}) exceeds policy {d}",
                .{ level0_files, self.policy.max_level0_files },
            );
            self.queueAction(.{ .compact_level = .{ .level = 0 } });
        }

        // Check other levels based on size ratios
        for (1..db.sstables.num_levels) |level| {
            const level_stats = db.sstables.getLevelStats(level) orelse continue;
            const next_level_stats = db.sstables.getLevelStats(level + 1) orelse continue;

            // If this level is getting too full compared to the next level
            if (level_stats.files_count > 0 and next_level_stats.files_count > 0) {
                const ratio = @as(f64, @floatFromInt(level_stats.files_count)) /
                    @as(f64, @floatFromInt(next_level_stats.files_count));

                if (ratio > self.policy.compaction_level_threshold) {
                    std.log.debug(
                        "state evaluated: level {d} file ratio ({d}) exceeds policy {d}",
                        .{ level, ratio, self.policy.compaction_level_threshold },
                    );
                    self.queueAction(.{ .compact_level = .{ .level = level } });
                }
            }
        }
    }

    fn considerFreezingMemtable(self: *Self, data: u64) void {
        const db: *Database = @ptrCast(@alignCast(self.db));

        var mtable = db.mtable.load(.seq_cst);
        if ((mtable.size() + data) >= db.capacity) {
            self.queueAction(.{ .freeze_memtable = .{ .id = mtable.getId() } });
        }
    }

    fn considerFlushingMemtable(self: *Self, id: []const u8) void {
        const db: *Database = @ptrCast(@alignCast(self.db));

        const count = db.memtableCount();
        const max_tables = self.policy.max_unflushed_memtables;

        // Simple policy: if we have too many unflushed memtables, flush this one
        if (count >= max_tables) {
            self.queueAction(.{ .flush_memtable = .{ .id = id } });
        }
    }

    fn considerCompactingLevel(self: *Self, level: usize) void {
        const db: *Database = @ptrCast(@alignCast(self.db));

        // Check if this level needs compaction based on file count
        const files_count = db.sstables.stats.getFilesCount(level);
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
        const db: *Database = @ptrCast(@alignCast(self.db));

        if (idle_ms < self.policy.idle_compact_threshold_ms) return;

        // Find a level that hasn't been compacted in a while
        var oldest_level: usize = 0;
        var oldest_time: i64 = std.time.timestamp();

        for (0..db.sstables.num_levels) |level| {
            const last_compaction = db.sstables.stats.getLastCompactionTime(level);
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
