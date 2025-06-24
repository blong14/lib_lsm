const std = @import("std");

const clap = @import("clap");
const jemalloc = @import("jemalloc");
const lsm = @import("lsm");

const debug = std.debug;
const fs = std.fs;
const heap = std.heap;
const io = std.io;
const mem = std.mem;

const Allocator = mem.Allocator;

const KV = lsm.KV;

const allocator = jemalloc.allocator;
const usage =
    \\-h, --help             Display this help and exit.
    \\-d, --data_dir <str>   The data directory to save files on disk.
    \\-i, --input    <str>   An input file to import. Only supports csv.
    \\-b, --bench            Run the benchmark tests.
    \\--sst_capacity <usize> Max capacity for an SST block.
    \\
;

pub const std_options = .{
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

    // Initialize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, parsers, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        diag.report(io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        std.log.info("{s}", .{usage});
        return;
    }

    const default_opts = lsm.defaultOpts();

    const data_dir = res.args.data_dir orelse default_opts.data_dir;
    const sst_capacity = res.args.sst_capacity orelse default_opts.sst_capacity;
    const wal_capacity = default_opts.wal_capacity;

    const opts: lsm.Opts = .{
        .compaction_strategy = .simple,
        .data_dir = data_dir,
        .enable_agent = true,
        .num_levels = 3,
        .sst_capacity = sst_capacity,
        .wal_capacity = wal_capacity,
    };

    const db = lsm.databaseFromOpts(allocator, opts) catch |err| {
        std.log.err("database init error {s}", .{@errorName(err)});
        return err;
    };
    defer allocator.destroy(db);
    defer db.deinit(allocator);

    try db.open(allocator);

    if (res.args.bench != 0) {
        benchmark(allocator, db);
    } else {
        // Fallback runnable used for simple scanning of the database files.
        iterator(allocator, db);
    }
}

fn benchmark(alloc: Allocator, db: *lsm.Database) void {
    const num_ops = 10_000;

    // Number of worker threads to use - adjust based on available cores
    const num_threads: u64 = @min(16, std.Thread.getCpuCount() catch 4);

    std.log.info("Starting benchmark with {d} operations using {d} threads...", .{ num_ops, num_threads });

    // Timing variables
    var timer = std.time.Timer.start() catch unreachable;
    var write_time: u64 = 0;
    var read_time: u64 = 1;

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
        const threads = alloc.alloc(ThreadContext, num_threads) catch unreachable;
        defer alloc.free(threads);

        const ops_per_thread = num_ops / num_threads;

        // Initialize thread contexts
        for (threads, 0..) |*ctx, i| {
            const start = i * ops_per_thread;
            const end = if (i == num_threads - 1) num_ops else start + ops_per_thread;
            ctx.* = ThreadContext.init(db, i, alloc, start, end);
        }

        // Worker function for writes
        const writeWorker = struct {
            fn work(ctx: *ThreadContext) void {
                var arena = lsm.ThreadSafeBumpAllocator.init(ctx.alloc, 1024 * 1024) catch unreachable;
                defer arena.deinit();

                const malloc = arena.allocator();

                var success_count: u64 = 0;
                var error_count: u64 = 0;

                for (ctx.start_idx..ctx.end_idx) |i| {
                    const key = std.fmt.allocPrint(malloc, "key_{d}", .{i}) catch unreachable;
                    const value = std.fmt.allocPrint(malloc, "value_{d}", .{i}) catch unreachable;

                    ctx.db.write(malloc, key, value) catch |err| {
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

        // Ensure all writes are flushed
        db.flush(alloc);

        std.log.info("Write phase completed: {d} successful, {d} errors", .{ total_success, total_errors });
    }

    // Read benchmark
    const should_run = true;
    if (should_run) {
        const threads = alloc.alloc(ThreadContext, num_threads) catch unreachable;
        defer alloc.free(threads);

        const ops_per_thread = num_ops / num_threads;

        // Initialize thread contexts
        for (threads, 0..) |*ctx, i| {
            const start = i * ops_per_thread;
            const end = if (i == num_threads - 1) num_ops else start + ops_per_thread;
            ctx.* = ThreadContext.init(db, i, alloc, start, end);
        }

        // Worker function for reads
        const readWorker = struct {
            fn work(ctx: *ThreadContext) void {
                var success_count: u64 = 0;
                var error_count: u64 = 0;
                const chunk_size = (ctx.end_idx - ctx.start_idx) / 5;

                var arena = lsm.ThreadSafeBumpAllocator.init(ctx.alloc, 1024 * 1024) catch unreachable;
                defer arena.deinit();

                const malloc = arena.allocator();

                for (ctx.start_idx..ctx.end_idx) |i| {
                    const key = std.fmt.allocPrint(malloc, "key_{d}", .{i}) catch unreachable;

                    _ = ctx.db.read(malloc, key) catch |err| {
                        std.log.debug("database read error for key {s} {s}", .{
                            key,
                            @errorName(err),
                        });
                        error_count += 1;
                        continue;
                    };

                    success_count += 1;

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
    }

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

fn scan(alloc: Allocator, db: *lsm.Database, start: db.KV, end: db.KV) void {
    var it = db.scan(alloc, start, end) catch |err| @panic(@errorName(err));
    defer it.deinit();

    var w = std.io.bufferedWriter(std.io.getStdOut().writer());
    defer w.flush() catch unreachable;

    var count: usize = 0;
    while (it.next()) |kv| {
        count += 1;
        const out = std.fmt.allocPrint(alloc, "{s}\n", .{kv}) catch unreachable;
        _ = w.write(out) catch unreachable;
    }

    std.log.info("\ntotal rows {d}", .{count});
}

fn iterator(alloc: Allocator, db: *lsm.Database) void {
    var it = db.iterator(alloc) catch |err| @panic(@errorName(err));
    defer it.deinit();

    // var w = std.io.bufferedWriter(std.io.getStdOut().writer());

    // var arena = lsm.ThreadSafeBumpAllocator.init(alloc, 1024 * 1024) catch unreachable;
    // defer arena.deinit();

    // const malloc = arena.allocator();

    var count: usize = 0;
    while (it.next()) |kv| {
        _ = kv;
        count += 1;
        // const out = std.fmt.allocPrint(malloc, "{s}\n", .{kv}) catch unreachable;
        // _ = w.write(out) catch unreachable;
    }

    // w.flush() catch unreachable;

    std.log.info("\ntotal rows {d}", .{count});
}
