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
    defer db.deinit();

    try db.open();

    if (res.args.bench != 0) {
        benchmark(allocator, db);
    } else {
        // Fallback runnable used for simple scanning of the database files.
        scanAndPrint(allocator, db);
    }
}

fn benchmark(alloc: Allocator, db: *lsm.Database) void {
    // Use a smaller number of operations to avoid freezing
    const num_ops = 1_000_000;

    std.log.info("Starting benchmark with {d} operations...", .{num_ops});

    // Timing variables
    var timer = std.time.Timer.start() catch unreachable;
    var write_time: u64 = 0;
    var read_time: u64 = 0;

    // Write benchmark
    var count: u64 = 0;
    timer.reset();
    for (0..num_ops) |i| {
        const key = std.fmt.allocPrint(alloc, "key_{d}", .{i}) catch unreachable;
        defer alloc.free(key);

        const value = std.fmt.allocPrint(alloc, "value_{d}", .{i}) catch unreachable;
        defer alloc.free(value);

        count += 1;
        db.write(key, value) catch |err| {
            std.log.err("Write error: {s}", .{@errorName(err)});
            return;
        };

        // Periodically log progress to show the benchmark is still running
        if (i % (num_ops / 10) == 0 and i > 0) {
            std.log.info("Write progress ({d}): {d}%", .{ count, i * 100 / num_ops });
        }
    }

    write_time = timer.read();
    std.log.info("Write phase completed: total keys {d}", .{count});

    // Read benchmark
    count = 0;
    timer.reset();
    for (0..num_ops) |i| {
        const key = std.fmt.allocPrint(alloc, "key_{d}", .{i}) catch unreachable;
        defer alloc.free(key);

        count += 1;
        _ = db.read(key) catch continue;

        // Periodically log progress
        if (i % (num_ops / 10) == 0 and i > 0) {
            std.log.info("Read progress({d}): {d}%", .{ count, i * 100 / num_ops });
        }
    }
    read_time = timer.read();

    std.log.info("Read phase completed: total keys {d}", .{count});

    // Report results
    const write_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(write_time)) / std.time.ns_per_s);
    const read_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(read_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Write: {d:.2} ops/sec ({d:.2} ms total)", .{ write_ops_per_sec, @as(f64, @floatFromInt(write_time)) / std.time.ns_per_ms });
    std.log.info("  Read:  {d:.2} ops/sec ({d:.2} ms total)", .{ read_ops_per_sec, @as(f64, @floatFromInt(read_time)) / std.time.ns_per_ms });
}

fn scanAndPrint(alloc: Allocator, db: *lsm.Database) void {
    var it = db.iterator(alloc) catch |err| @panic(@errorName(err));
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |kv| {
        count += 1;
        std.log.info("{s}", .{kv});
    }

    std.log.info("total rows {d}", .{count});
}
