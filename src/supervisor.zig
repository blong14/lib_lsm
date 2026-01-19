const std = @import("std");

const Pool = std.Thread.Pool;
const Database = @import("Database.zig");
const KV = @import("KV.zig");

/// Events that can trigger supervisor decisions
pub const DatabaseEvent = union(enum) {
    write_completed: struct { bytes: u64 },
    read_completed: struct { bytes: u64 },
    sstable_created: struct { level: usize },
    compaction_completed: struct { level: usize, duration_ns: i64 },
    system_idle: struct { duration_ms: u64 },
};

/// Actions the supervisor can decide to take
pub const SupervisorAction = union(enum) {
    flush_memtable: struct { id: u64 },
    no_action: void,
};

/// Configurable policy parameters
pub const SupervisorPolicy = struct {
    max_level0_files: u32 = 16,
    max_unflushed_memtables: usize = 2,
    min_write_cooldown_ms: u64 = 500,
    compaction_level_threshold: f64 = 0.8,
    idle_compact_threshold_ms: u64 = 5000,
};

pub const DatabaseSupervisor = struct {
    alloc: std.mem.Allocator,
    db: *Database,
    policy: SupervisorPolicy,

    action_mutex: std.Thread.Mutex = .{},
    action_queue: std.ArrayList(SupervisorAction),

    event_mutex: std.Thread.Mutex = .{},
    event_queue: std.ArrayList(DatabaseEvent),

    running: std.atomic.Value(bool),

    pool: ?*Pool = null,

    const Self = @This();

    pub fn init(alloc: std.mem.Allocator, db: *Database, policy: SupervisorPolicy) !*Self {
        const supervisor = try alloc.create(Self);
        supervisor.* = .{
            .alloc = alloc,
            .db = db,
            .policy = policy,
            .event_queue = try std.ArrayList(DatabaseEvent).initCapacity(alloc, 256),
            .action_queue = try std.ArrayList(SupervisorAction).initCapacity(alloc, 256),
            .running = std.atomic.Value(bool).init(false),
        };
        return supervisor;
    }

    pub fn deinit(self: *Self) void {
        self.event_queue.deinit(self.alloc);
        self.action_queue.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn start(self: *Self) !void {
        if (self.isRunning()) return;

        self.running.store(true, .release);

        var thread_pool = try self.alloc.create(Pool);
        try thread_pool.init(Pool.Options{
            .allocator = self.alloc,
            .n_jobs = 16, // TODO: make configurable
        });
        errdefer thread_pool.deinit();

        try thread_pool.spawn(Self.run, .{self});

        self.pool = thread_pool;
    }

    pub fn read(self: Self, key: []const u8) !?KV {
        if (!self.isRunning()) return error.NotRunning;

        const pool = self.pool orelse return error.MissingPool;

        const ThreadContext = struct {
            db: *Database,
            key: []const u8,
            mtx: std.Thread.Mutex = .{},
            value: ?KV = null,
            err: ?anyerror = null,
        };

        const reader = struct {
            fn read(ctx: *ThreadContext) void {
                ctx.mtx.lock();
                defer ctx.mtx.unlock();

                const kv = ctx.db.read(ctx.key) catch |err| {
                    ctx.*.err = err;
                    return;
                };

                if (kv) |v| {
                    ctx.*.value = v;
                }
            }
        }.read;

        var wait_group = std.Thread.WaitGroup{};
        wait_group.reset();

        var ctx: ThreadContext = .{ .db = self.db, .key = key };

        pool.spawnWg(&wait_group, reader, .{&ctx});
        pool.waitAndWork(&wait_group);

        return ctx.value;
    }

    pub fn write(self: Self, kv: KV) !void {
        if (!self.isRunning()) return error.NotRunning;

        const pool = self.pool orelse return error.MissingPool;

        const ThreadContext = struct {
            db: *Database,
            value: KV,
        };

        const writer = struct {
            fn write(ctx: ThreadContext) void {
                ctx.db.write(ctx.value) catch |err| {
                    std.log.err("{s}", .{@errorName(err)});
                };
            }
        }.write;

        var wait_group = std.Thread.WaitGroup{};
        wait_group.reset();

        const ctx: ThreadContext = .{ .db = self.db, .value = kv };

        pool.spawnWg(&wait_group, writer, .{ctx});
        pool.waitAndWork(&wait_group);

        return;
    }

    pub fn stop(self: *Self) void {
        if (!self.isRunning()) return;

        self.running.store(false, .release);

        if (self.pool) |pool| {
            pool.deinit();
            self.pool = null;
        }
    }

    pub fn isRunning(self: Self) bool {
        return self.running.load(.acquire);
    }

    fn run(self: *Self) void {
        const target_frame_time_ns: i128 = @divTrunc(std.time.ns_per_s, 120);

        while (self.isRunning()) {
            const frame_start = std.time.nanoTimestamp();

            if (!self.isRunning()) break;

            self.processEvents();

            if (!self.isRunning()) break;

            self.processActions();

            const frame_time = std.time.nanoTimestamp() - frame_start;

            if (frame_time < target_frame_time_ns) {
                const sleep_time_ns = target_frame_time_ns - frame_time;
                std.Thread.sleep(@intCast(sleep_time_ns));
            } else if (frame_time > target_frame_time_ns * 2) {
                const frame_time_ms = @divFloor(frame_time, std.time.ns_per_ms);
                std.log.warn("frame time ({d:.2}ms) exceeded target ({d:.2}ms) by more than 2x", .{
                    frame_time_ms,
                    @divFloor(target_frame_time_ns, std.time.ns_per_ms),
                });
            }
        }
    }

    fn queueAction(self: *Self, action: SupervisorAction) void {
        self.action_mutex.lock();
        defer self.action_mutex.unlock();

        self.action_queue.append(self.alloc, action) catch |err| {
            std.log.err("failed to queue action: {s}", .{@errorName(err)});
        };
    }

    fn processActions(self: *Self) void {
        self.action_mutex.lock();
        defer self.action_mutex.unlock();

        if (self.action_queue.items.len > 0) {
            const action = self.action_queue.orderedRemove(0);
            switch (action) {
                .flush_memtable => |_| {
                    std.log.debug("flushing_memtable", .{});

                    self.db.flush(self.alloc) catch |err| {
                        std.log.err(
                            "supervisor not able to flush database {s}",
                            .{@errorName(err)},
                        );
                        return;
                    };

                    self.submitEvent(.{ .sstable_created = .{ .level = 0 } });
                },
                .no_action => return,
            }
        }
    }

    pub fn submitEvent(self: *Self, event: DatabaseEvent) void {
        self.event_mutex.lock();
        defer self.event_mutex.unlock();

        self.event_queue.append(self.alloc, event) catch |err| {
            std.log.err("failed to submit event: {s}", .{@errorName(err)});
        };
    }

    fn processEvents(self: *Self) void {
        self.event_mutex.lock();
        defer self.event_mutex.unlock();

        if (self.event_queue.items.len > 0) {
            const event = self.event_queue.orderedRemove(0);
            switch (event) {
                .write_completed => |data| {
                    // std.log.debug("write_completed", .{});

                    self.considerFlushingMemtable(data.bytes);
                },
                .sstable_created => |data| {
                    std.log.debug("sstable_created", .{});

                    _ = data;
                },
                .compaction_completed => |data| {
                    std.log.debug("compaction_completed", .{});

                    _ = data;
                },
                .read_completed => |data| {
                    // std.log.debug("read_completed", .{});

                    _ = data;
                },
                .system_idle => |data| {
                    std.log.debug("system_idle", .{});

                    _ = data;
                },
            }
        }
    }

    fn considerFlushingMemtable(self: *Self, data: u64) void {
        if (self.db.transaction() catch return) |txn| {
            defer txn.release();

            if ((txn.write_buffer.size() + data) >= self.db.Opts.sst_capacity) {
                self.queueAction(.{ .flush_memtable = .{ .id = txn.write_buffer.getId() } });
            }
        }
    }
};
