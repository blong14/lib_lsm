const std = @import("std");

const clap = @import("clap");
const csv = @cImport({
    @cInclude("csv.h");
});
const lsm = @import("lsm");

const debug = std.debug;
const fs = std.fs;
const heap = std.heap;
const io = std.io;
const mem = std.mem;

const Allocator = mem.Allocator;
const KV = lsm.KV;

var tsa: std.heap.ThreadSafeAllocator = .{
    .child_allocator = std.heap.c_allocator,
};
const usage =
    \\-h, --help             Display this help and exit.
    \\-d, --data_dir <str>   The data directory to save files on disk.
    \\-i, --input    <str>   An input file to import. Only supports csv.
    \\-w, --write            Run the write only tests.
    \\-r, --read             Run the read only tests.
    \\-s, --scan             Run the read and scan tests.
    \\-b, --bench            Run the benchmark tests.
    \\-p, --perf             Run the debug perf tests.
    \\--sst_capacity <usize> Max capacity for an SST block.
    \\
;

pub const std_options: std.Options = .{
    .log_level = .debug,
};

pub fn main() !void {
    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`
    const params = comptime clap.parseParamsComptime(usage);

    const parsers = comptime .{
        .str = clap.parsers.string,
        .usize = clap.parsers.int(usize, 10),
    };

    const allocator = tsa.allocator();

    // Initialize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, parsers, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        var buf: [1024]u8 = undefined;
        var w = std.fs.File.stderr().writer(&buf).interface;
        diag.report(&w, err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        std.log.info("{s}", .{usage});
        return;
    }

    const default_opts = lsm.defaultOpts();

    const data_dir = res.args.data_dir orelse default_opts.data_dir;
    // const sst_capacity = res.args.sst_capacity orelse default_opts.sst_capacity;
    const sst_capacity = default_opts.sst_capacity;
    const wal_capacity = default_opts.wal_capacity;

    const opts: lsm.Opts = .{
        .compaction_strategy = .simple,
        .data_dir = data_dir,
        .enable_agent = true,
        .num_levels = 3,
        .sst_capacity = sst_capacity,
        .wal_capacity = wal_capacity,
    };

    const db = try lsm.init(allocator, opts);
    defer lsm.deinit(allocator, db);

    if (res.args.read != 0) {
        read(allocator, db, res.args.input.?);
    } else if (res.args.write != 0) {
        write(allocator, db, res.args.input.?);
    } else if (res.args.bench != 0) {
        xbenchmark(allocator, db);
        benchmark(allocator, db);
    } else if (res.args.perf != 0) {
        write(allocator, db, res.args.input.?);
        read(allocator, db, res.args.input.?);
        // scan(allocator, db, "Atlanta", "New York");
    } else {
        // Fallback runnable used for simple scanning of the database files.
        scan(allocator, db, "Atlanta", "New York");
    }
}

fn read(alloc: Allocator, db: *lsm.Database, input: []const u8) void {
    const num_ops = 1_000_000;
    const num_cpus: u64 = std.Thread.getCpuCount() catch 4;
    const ops_per_thread = num_ops / num_cpus;

    std.log.info("Starting read tests with {d} operations per thread...", .{ops_per_thread});

    // Timing variables
    var timer = std.time.Timer.start() catch unreachable;
    var read_time: u64 = 0;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    // Used to manage benchmark memory
    const arena_alloc = arena.allocator();
    const ReadThreadContext = struct {
        wg: *std.Thread.WaitGroup,
        thread_id: usize,
        db: *lsm.Database,
        alloc: Allocator,
        items: [][]const u8,
        success_count: std.atomic.Value(u64),
        error_count: std.atomic.Value(u64),

        fn init(
            malloc: Allocator,
            wg: *std.Thread.WaitGroup,
            thread_id: usize,
            database: *lsm.Database,
            items: [][]const u8,
        ) @This() {
            return .{
                .alloc = malloc,
                .wg = wg,
                .thread_id = thread_id,
                .db = database,
                .items = items,
                .success_count = std.atomic.Value(u64).init(0),
                .error_count = std.atomic.Value(u64).init(0),
            };
        }
    };

    const readWorker = struct {
        fn read(ctx: *ReadThreadContext) void {
            ctx.wg.start();
            defer ctx.wg.finish();

            var success_count: u64 = 0;
            var error_count: u64 = 0;

            for (ctx.items, 0..) |key, i| {
                const kv = lsm.read(ctx.db, key) catch |err| {
                    std.log.err("database read error for key '{s}' (len={d}): {s}", .{
                        key,
                        key.len,
                        @errorName(err),
                    });
                    error_count += 1;
                    continue;
                };

                if (kv) |_| {
                    success_count += 1;
                }

                // Periodically log progress
                const chunk_size = ctx.items.len / 5;
                if (chunk_size > 0 and i % chunk_size == 0 and i > 0) {
                    const progress = i * 100 / ctx.items.len;
                    std.log.debug("Thread {d} read progress: {d}%", .{
                        ctx.thread_id,
                        progress,
                    });
                }
            }

            ctx.success_count.store(success_count, .release);
            ctx.error_count.store(error_count, .release);
        }
    }.read;

    var kvs = std.ArrayList([]const u8).initCapacity(arena_alloc, 4096) catch unreachable;
    defer kvs.deinit(arena_alloc);

    var threads = std.ArrayList(*ReadThreadContext).initCapacity(arena_alloc, 10) catch unreachable;
    defer threads.deinit(arena_alloc);

    var wait_group = arena_alloc.create(std.Thread.WaitGroup) catch unreachable;
    wait_group.reset();

    const Pool = std.Thread.Pool;

    var thread_pool: Pool = undefined;
    thread_pool.init(Pool.Options{ .allocator = arena_alloc }) catch |err| {
        debug.print(
            "threadpool init error {s}\n",
            .{@errorName(err)},
        );
    };
    defer thread_pool.deinit();

    const handle = csv.CsvOpen2(input.ptr, ';', '"', '\\');
    defer csv.CsvClose(handle);

    var thread_id: usize = 0;

    // Start timer and launch threads
    timer.reset();

    var i: usize = 0;
    while (csv.CsvReadNextRow(handle)) |row| {
        if (csv.CsvReadNextCol(row, handle)) |val| {
            defer i += 1;

            const k = mem.span(val);
            const key = std.fmt.allocPrint(arena_alloc, "{s}_{d}", .{ k, i }) catch unreachable;

            kvs.append(arena_alloc, key) catch return;
        }

        if (kvs.items.len >= ops_per_thread) {
            const items = kvs.toOwnedSlice(arena_alloc) catch |err| {
                debug.print(
                    "not able to publish items {s}\n",
                    .{@errorName(err)},
                );
                return;
            };

            const ctx = arena_alloc.create(ReadThreadContext) catch unreachable;
            ctx.* = ReadThreadContext.init(arena_alloc, wait_group, thread_id, db, items);

            thread_pool.spawn(readWorker, .{ctx}) catch |err| {
                debug.print(
                    "threadpool spawn error {s}\n",
                    .{@errorName(err)},
                );
                return;
            };

            thread_id += 1;
            threads.append(arena_alloc, ctx) catch unreachable;
        }
    }

    if (kvs.items.len > 0) {
        const items = kvs.toOwnedSlice(arena_alloc) catch |err| {
            debug.print(
                "not able to publish items {s}\n",
                .{@errorName(err)},
            );
            return;
        };

        const ctx = arena_alloc.create(ReadThreadContext) catch unreachable;
        ctx.* = ReadThreadContext.init(arena_alloc, wait_group, thread_id, db, items);

        thread_pool.spawn(readWorker, .{ctx}) catch |err| {
            debug.print(
                "threadpool spawn error {s}\n",
                .{@errorName(err)},
            );
            return;
        };

        thread_id += 1;
        threads.append(arena_alloc, ctx) catch unreachable;
    }

    // stutter to make sure threads are schehduled. must be a better way
    std.Thread.sleep(1000);

    thread_pool.waitAndWork(wait_group);

    read_time = timer.read();

    // Calculate total counts
    var total_success: u64 = 0;
    var total_errors: u64 = 0;

    for (threads.items) |ctx| {
        total_success += ctx.success_count.load(.acquire);
        total_errors += ctx.error_count.load(.acquire);
    }

    std.log.info("Read phase completed: {d} workers {d} successful, {d} errors", .{ thread_id, total_success, total_errors });

    const read_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(read_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Read:  {d:.2} ops/sec ({d:.2} ms total)", .{
        read_ops_per_sec, @as(f64, @floatFromInt(read_time)) / std.time.ns_per_ms,
    });
}

fn write(alloc: Allocator, db: *lsm.Database, input: []const u8) void {
    const num_ops = 1_000_000;
    const num_cpus: u64 = std.Thread.getCpuCount() catch 4;
    const ops_per_thread = num_ops / num_cpus;
    const write_cnt = ops_per_thread;

    std.log.info("Starting write tests with {d} operations per thread...", .{ops_per_thread});

    // Timing variables
    var timer = std.time.Timer.start() catch unreachable;
    var write_time: u64 = 0;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    // Used to manage benchmark memory
    const arena_alloc = arena.allocator();

    const ThreadContext = struct {
        wg: *std.Thread.WaitGroup,
        thread_id: usize,
        db: *lsm.Database,
        alloc: Allocator,
        items: []const KV,
        success_count: std.atomic.Value(u64),
        error_count: std.atomic.Value(u64),

        fn init(
            malloc: Allocator,
            wg: *std.Thread.WaitGroup,
            thread_id: usize,
            database: *lsm.Database,
            items: []const KV,
        ) @This() {
            return .{
                .alloc = malloc,
                .wg = wg,
                .thread_id = thread_id,
                .db = database,
                .items = items,
                .success_count = std.atomic.Value(u64).init(0),
                .error_count = std.atomic.Value(u64).init(0),
            };
        }
    };

    const writeWorker = struct {
        fn work(ctx: *ThreadContext) void {
            ctx.wg.start();
            defer ctx.wg.finish();

            var success_count: u64 = 0;
            var error_count: u64 = 0;

            for (ctx.items, 0..) |kv, i| {
                lsm.write(ctx.db, kv) catch |err| {
                    std.log.debug("database write error for key {s} {s}\n", .{
                        kv.key,
                        @errorName(err),
                    });
                    error_count += 1;
                    @panic(@errorName(err));
                };

                success_count += 1;

                // Periodically log progress
                const chunk_size = ctx.items.len / 5;
                if (chunk_size > 0 and i % chunk_size == 0 and i > 0) {
                    const progress = i * 100 / ctx.items.len;
                    std.log.debug("Thread {d} write progress: {d}%", .{
                        ctx.thread_id,
                        progress,
                    });
                }
            }

            ctx.success_count.store(success_count, .release);
            ctx.error_count.store(error_count, .release);
        }
    }.work;

    var threads = std.ArrayList(*ThreadContext).initCapacity(arena_alloc, 10) catch unreachable;
    defer threads.deinit(arena_alloc);

    var wait_group = arena_alloc.create(std.Thread.WaitGroup) catch unreachable;
    wait_group.reset();

    const Pool = std.Thread.Pool;

    var thread_pool: Pool = undefined;
    thread_pool.init(Pool.Options{ .allocator = arena_alloc }) catch |err| {
        debug.print(
            "threadpool init error {s}\n",
            .{@errorName(err)},
        );
    };
    defer thread_pool.deinit();

    var kvs = std.ArrayList(KV).initCapacity(arena_alloc, 4096) catch unreachable;
    defer kvs.deinit(arena_alloc);

    const handle = csv.CsvOpen2(input.ptr, ';', '"', '\\');
    defer csv.CsvClose(handle);

    var thread_id: usize = 0;

    // Start timer and launch threads
    timer.reset();

    var i: usize = 0;
    while (csv.CsvReadNextRow(handle)) |row| {
        defer i += 1;
        var k: []const u8 = undefined;
        if (csv.CsvReadNextCol(row, handle)) |val| {
            k = mem.span(val);
        } else {
            break;
        }

        var value: []const u8 = undefined;
        if (csv.CsvReadNextCol(row, handle)) |val| {
            value = mem.span(val);
        } else {
            break;
        }

        const key = std.fmt.allocPrint(arena_alloc, "{s}_{d}", .{ k, i }) catch unreachable;

        const item = KV.initOwned(arena_alloc, key, value) catch unreachable;
        kvs.append(arena_alloc, item) catch return;

        if (kvs.items.len >= write_cnt) {
            const items = kvs.toOwnedSlice(arena_alloc) catch |err| {
                debug.print(
                    "not able to publish items {s}\n",
                    .{@errorName(err)},
                );
                return;
            };

            const ctx = arena_alloc.create(ThreadContext) catch unreachable;
            // pass in original allocator here not the arena
            ctx.* = ThreadContext.init(alloc, wait_group, thread_id, db, items);

            thread_pool.spawn(writeWorker, .{ctx}) catch |err| {
                debug.print(
                    "threadpool spawn error {s}\n",
                    .{@errorName(err)},
                );
                return;
            };

            thread_id += 1;
            threads.append(arena_alloc, ctx) catch unreachable;
        }
    }

    if (kvs.items.len > 0) {
        const items = kvs.toOwnedSlice(arena_alloc) catch |err| {
            debug.print(
                "not able to publish items {s}\n",
                .{@errorName(err)},
            );
            return;
        };

        const ctx = arena_alloc.create(ThreadContext) catch unreachable;
        // pass in original allocator here not the arena
        ctx.* = ThreadContext.init(alloc, wait_group, thread_id, db, items);

        thread_pool.spawn(writeWorker, .{ctx}) catch |err| {
            debug.print(
                "threadpool spawn error {s}\n",
                .{@errorName(err)},
            );
            return;
        };

        threads.append(arena_alloc, ctx) catch unreachable;
    }

    std.Thread.sleep(1000);
    thread_pool.waitAndWork(wait_group);

    // Calculate total counts
    var total_success: u64 = 0;
    var total_errors: u64 = 0;

    for (threads.items) |ctx| {
        total_success += ctx.success_count.load(.acquire);
        total_errors += ctx.error_count.load(.acquire);
    }

    write_time = timer.read();

    std.log.info("Write phase completed: {d} workers {d} successful, {d} errors", .{ thread_id, total_success, total_errors });

    const write_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(write_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Write: {d:.2} ops/sec ({d:.2} ms total)", .{
        write_ops_per_sec, @as(f64, @floatFromInt(write_time)) / std.time.ns_per_ms,
    });
}

fn benchmark(alloc: Allocator, db: *lsm.Database) void {
    const num_ops = 1_000_000;
    const num_cpus: u64 = std.Thread.getCpuCount() catch 4;
    const ops_per_thread = num_ops / num_cpus;

    std.log.info("Starting benchmark with {d} operations per thread...", .{ops_per_thread});

    // Timing variables
    var timer = std.time.Timer.start() catch unreachable;
    var write_time: u64 = 0;
    var read_time: u64 = 0;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    // Used to manage benchmark memory
    const arena_alloc = arena.allocator();

    // Thread synchronization
    const ThreadContext = struct {
        thread: std.Thread = undefined,
        thread_id: usize,
        db: *lsm.Database,
        alloc: Allocator,
        start_idx: usize,
        end_idx: usize,
        success_count: std.atomic.Value(u64),
        error_count: std.atomic.Value(u64),

        fn init(
            database: *lsm.Database,
            thread_id: usize,
            malloc: Allocator,
            start: usize,
            end: usize,
        ) @This() {
            return .{
                .db = database,
                .thread_id = thread_id,
                .alloc = malloc,
                .start_idx = start,
                .end_idx = end,
                .success_count = std.atomic.Value(u64).init(0),
                .error_count = std.atomic.Value(u64).init(0),
            };
        }
    };

    // Write benchmark
    {
        const threads = arena_alloc.alloc(ThreadContext, num_cpus) catch unreachable;

        // Initialize thread contexts
        for (threads, 0..) |*ctx, i| {
            const start = i * ops_per_thread;
            const end = if (i == num_cpus - 1) num_ops else start + ops_per_thread;
            ctx.* = ThreadContext.init(db, i, alloc, start, end);
        }

        // Worker function for writes
        const writeWorker = struct {
            fn work(ctx: *ThreadContext) void {
                const malloc = ctx.alloc;

                var success_count: u64 = 0;
                var error_count: u64 = 0;

                for (ctx.start_idx..ctx.end_idx) |i| {
                    const key = std.fmt.allocPrint(malloc, "key_{d}", .{i}) catch unreachable;
                    const value = std.fmt.allocPrint(malloc, "value_{d}", .{i}) catch unreachable;

                    const kv = KV.initOwned(malloc, key, value) catch unreachable;

                    lsm.write(ctx.db, kv) catch |err| {
                        std.log.debug("database write error for key {s} {s}\n", .{
                            key,
                            @errorName(err),
                        });
                        error_count += 1;
                        continue;
                    };

                    success_count += 1;

                    // Periodically log progress
                    const chunk_size = (ctx.end_idx - ctx.start_idx) / 5;
                    if (chunk_size > 0 and i % chunk_size == 0 and i > ctx.start_idx) {
                        const progress = (i - ctx.start_idx) * 100 / (ctx.end_idx - ctx.start_idx);
                        std.log.debug("Thread {d} write progress: {d}%", .{
                            ctx.thread_id,
                            progress,
                        });
                    }
                }

                ctx.success_count.store(success_count, .release);
                ctx.error_count.store(error_count, .release);
            }
        }.work;

        // Start timer and launch threads
        timer.reset();

        for (threads) |*ctx| {
            ctx.thread = std.Thread.spawn(.{}, writeWorker, .{ctx}) catch unreachable;
        }

        // Wait for all threads to complete
        for (threads) |*ctx| {
            ctx.thread.join();
        }

        // Calculate total counts
        var total_success: u64 = 0;
        var total_errors: u64 = 0;

        for (threads) |ctx| {
            total_success += ctx.success_count.load(.acquire);
            total_errors += ctx.error_count.load(.acquire);
        }

        write_time = timer.read();

        std.log.info("Write phase completed: {d} successful, {d} errors", .{ total_success, total_errors });
    }

    // Read benchmark
    const threads = arena_alloc.alloc(ThreadContext, num_cpus) catch unreachable;

    // Initialize thread contexts
    for (threads, 0..) |*ctx, i| {
        const start = i * ops_per_thread;
        const end = if (i == num_cpus - 1) num_ops else start + ops_per_thread;
        ctx.* = ThreadContext.init(db, i, arena_alloc, start, end);
    }

    // Worker function for reads
    const readWorker = struct {
        fn work(ctx: *ThreadContext) void {
            var success_count: u64 = 0;
            var error_count: u64 = 0;
            const chunk_size = (ctx.end_idx - ctx.start_idx) / 5;

            const malloc = ctx.alloc;

            for (ctx.start_idx..ctx.end_idx) |i| {
                const key = std.fmt.allocPrint(malloc, "key_{d}", .{i}) catch unreachable;

                const v = lsm.read(ctx.db, key) catch |err| {
                    std.log.debug("database read error for key {s} {s}", .{
                        key,
                        @errorName(err),
                    });
                    error_count += 1;
                    continue;
                };

                if (v) |_| {
                    success_count += 1;
                }

                // Periodically log progress
                if (chunk_size > 0 and i % chunk_size == 0 and i > ctx.start_idx) {
                    const progress = (i - ctx.start_idx) * 100 / (ctx.end_idx - ctx.start_idx);
                    std.log.debug("Thread {d} read progress: {d}% of {d}", .{
                        ctx.thread_id,
                        progress,
                        ctx.end_idx - ctx.start_idx,
                    });
                }
            }

            ctx.success_count.store(success_count, .release);
            ctx.error_count.store(error_count, .release);
        }
    }.work;

    timer.reset();

    std.log.info("Starting read phase with {d} operations using {d} threads...", .{
        num_ops,
        threads.len,
    });

    for (threads) |*ctx| {
        ctx.thread = std.Thread.spawn(.{}, readWorker, .{ctx}) catch unreachable;
    }

    for (threads) |*ctx| {
        ctx.thread.join();
    }

    var total_success: u64 = 0;
    var total_errors: u64 = 0;

    for (threads) |ctx| {
        total_success += ctx.success_count.load(.acquire);
        total_errors += ctx.error_count.load(.acquire);
    }

    read_time = timer.read();

    std.log.info("Read phase completed: {d} successful, {d} missed", .{ total_success, total_errors });

    const write_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(write_time)) / std.time.ns_per_s);
    const read_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(read_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Write: {d:.2} ops/sec ({d:.2} ms total)", .{
        write_ops_per_sec, @as(f64, @floatFromInt(write_time)) / std.time.ns_per_ms,
    });
    std.log.info("  Read:  {d:.2} ops/sec ({d:.2} ms total)", .{
        read_ops_per_sec, @as(f64, @floatFromInt(read_time)) / std.time.ns_per_ms,
    });
}

fn xbenchmark(alloc: Allocator, db: *lsm.Database) void {
    const num_ops = 1_000_000;

    std.log.info("Starting xbenchmark with {d} operations...", .{num_ops});

    var timer = std.time.Timer.start() catch unreachable;
    var write_time: u64 = 0;

    var success_count: u64 = 0;
    var error_count: u64 = 0;

    for (0..num_ops) |i| {
        const key = std.fmt.allocPrint(alloc, "key_{d}", .{i}) catch unreachable;
        const value = std.fmt.allocPrint(alloc, "value_{d}", .{i}) catch unreachable;

        const kv = KV.initOwned(alloc, key, value) catch unreachable;

        lsm.write(db, kv) catch |err| {
            std.log.debug("database write error for key {s} {s}\n", .{
                key,
                @errorName(err),
            });
            error_count += 1;
            continue;
        };

        success_count += 1;
    }

    write_time = timer.read();

    std.log.info("Write phase completed: {d} successful, {d} errors", .{ success_count, error_count });

    const write_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(write_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Write: {d:.2} ops/sec ({d:.2} ms total)", .{
        write_ops_per_sec, @as(f64, @floatFromInt(write_time)) / std.time.ns_per_ms,
    });

    success_count = 0;
    error_count = 0;

    var read_time: u64 = 0;
    timer.reset();

    var it = db.iterator(alloc) catch |err| @panic(@errorName(err));
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |kv| {
        _ = kv;
        count += 1;
    }

    read_time = timer.read();

    std.log.info("Read phase completed: read {d} key value pairs", .{count});

    const read_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(read_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Read: {d:.2} ops/sec ({d:.2} ms total)", .{
        read_ops_per_sec, @as(f64, @floatFromInt(read_time)) / std.time.ns_per_ms,
    });
}

fn scan(alloc: Allocator, db: *lsm.Database, start: []const u8, end: []const u8) void {
    var it = db.scan(alloc, start, end) catch |err| @panic(@errorName(err));
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |kv| {
        std.log.info("{d} - {s}", .{ count, kv.key });
        count += 1;
    }

    std.log.info("\ntotal rows {d}", .{count});
}

fn iterator(alloc: Allocator, db: *lsm.Database) void {
    var it = db.iterator(alloc) catch |err| @panic(@errorName(err));
    defer it.deinit();

    var w = std.fs.File.stdout().writer().interface;

    var count: usize = 0;
    while (it.next()) |kv| {
        w.print("{d} - {s}\n", .{ count, kv.key }) catch unreachable;
        count += 1;
    }

    w.flush() catch unreachable;

    std.log.info("\ntotal rows {d}", .{count});
}
