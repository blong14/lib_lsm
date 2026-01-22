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

var allocator = std.heap.smp_allocator;

const usage =
    \\-h, --help             Display this help and exit.
    \\-d, --data_dir <str>   The data directory to save files on disk.
    \\-i, --input    <str>   An input file to import. Only supports csv.
    \\-w, --write            Run the write only tests.
    \\-r, --read             Run the read only tests.
    \\-s, --scan             Run the read and scan tests.
    \\-b, --bench            Run the benchmark tests.
    \\-p, --perf             Run the debug perf tests.
    \\--debug                Run the debug build tests. 
    \\--sst_capacity <usize> Max capacity for an SST block.
    \\
;

pub const std_options: std.Options = .{
    .log_level = .debug,
};

const KVCSV = struct {
    alloc: Allocator,
    handle: csv.CsvHandle,
    idx: usize = 0,

    pub fn init(alloc: Allocator, handle: csv.CsvHandle) KVCSV {
        return .{
            .alloc = alloc,
            .handle = handle,
        };
    }

    pub fn deinit(self: *KVCSV) void {
        self.* = undefined;
    }

    pub fn next(self: *KVCSV) ?KV {
        const row = csv.CsvReadNextRow(self.handle) orelse return null;

        const k_raw = csv.CsvReadNextCol(row, self.handle) orelse return null;
        const k = mem.span(k_raw);

        const value_raw = csv.CsvReadNextCol(row, self.handle) orelse return null;
        const value = mem.span(value_raw);

        var key_buf: [256]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}_{d}", .{ k, self.idx }) catch |err| {
            std.log.err("Failed to format key: {}", .{err});
            return null;
        };

        const item = KV.init(self.alloc, key, value) catch |err| {
            std.log.err("Failed to create KV item: {}", .{err});
            return null;
        };

        self.idx += 1;
        return item;
    }
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
        // .compaction_strategy = .simple,
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
        benchmark(allocator, db);
    } else if (res.args.perf != 0 or res.args.debug != 0) {
        write(allocator, db, res.args.input.?);
        read(allocator, db, res.args.input.?);
    } else {
        // Fallback runnable used for simple scanning of the database files.
        read(allocator, db, res.args.input.?);
    }
}

fn read(alloc: Allocator, db: *lsm.Database, input: []const u8) void {
    var success_count: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;
    var read_time: u64 = 0;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    // Used to manage benchmark memory
    const arena_alloc = arena.allocator();

    const handle = csv.CsvOpen2(input.ptr, ';', '"', '\\');
    defer csv.CsvClose(handle);

    var it: KVCSV = .init(arena_alloc, handle);
    defer it.deinit();

    while (it.next()) |nxt| {
        const kv = lsm.read(db, nxt.key) catch |err| {
            @panic(@errorName(err));
        };

        if (kv) |_| success_count += 1;
    }

    read_time = timer.read();

    std.log.info("Read phase completed: {d} successful", .{success_count});

    const read_ops_per_sec = @as(f64, @floatFromInt(success_count)) / (@as(f64, @floatFromInt(read_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Read:  {d:.2} ops/sec ({d:.2} ms total)", .{
        read_ops_per_sec, @as(f64, @floatFromInt(read_time)) / std.time.ns_per_ms,
    });
}

fn write(alloc: Allocator, db: *lsm.Database, input: []const u8) void {
    var success_count: usize = 0;

    var timer = std.time.Timer.start() catch unreachable;
    var write_time: u64 = 0;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    // Used to manage benchmark memory
    const arena_alloc = arena.allocator();

    const handle = csv.CsvOpen2(input.ptr, ';', '"', '\\');
    defer csv.CsvClose(handle);

    var it: KVCSV = .init(arena_alloc, handle);
    defer it.deinit();

    while (it.next()) |nxt| {
        lsm.write(db, nxt) catch |err| {
            @panic(@errorName(err));
        };

        success_count += 1;
    }

    write_time = timer.read();

    std.log.info("Write phase completed: {d} successful", .{success_count});

    const write_ops_per_sec = @as(f64, @floatFromInt(success_count)) / (@as(f64, @floatFromInt(write_time)) / std.time.ns_per_s);

    std.log.info("Benchmark Results:", .{});
    std.log.info("  Write: {d:.2} ops/sec ({d:.2} ms total)", .{
        write_ops_per_sec, @as(f64, @floatFromInt(write_time)) / std.time.ns_per_ms,
    });
}

fn benchmark(alloc: Allocator, db: *lsm.Database) void {
    const num_ops = 1_000_000;

    std.log.info("Starting benchmark with {d} operations...", .{num_ops});

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    const key_alloc = arena.allocator();

    var write_time: u64 = 0;

    var success_count: u64 = 0;
    var error_count: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;
    for (0..num_ops) |i| {
        var buffer: [64]u8 = undefined;
        const key = std.fmt.bufPrint(buffer[0..32], "key_{d}", .{i}) catch unreachable;
        const value = std.fmt.bufPrint(buffer[32..], "value_{d}", .{i}) catch unreachable;

        const kv = KV.init(key_alloc, key, value) catch unreachable;

        lsm.write(db, kv) catch |err| {
            std.log.debug("database write error for key {s} {s}\n", .{
                key,
                @errorName(err),
            });
            error_count += 1;
            continue;
        };

        success_count += 1;
        _ = arena.reset(.retain_capacity);
    }

    write_time = timer.read();

    const write_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(write_time)) / std.time.ns_per_s);

    std.log.info("Write Benchmark Results:", .{});
    std.log.info("  Total {d} Errors: {d} Write: {d:.2} ops/sec ({d:.2} ms total)", .{
        success_count, error_count, write_ops_per_sec, @as(f64, @floatFromInt(write_time)) / std.time.ns_per_ms,
    });

    success_count = 0;
    error_count = 0;

    var read_time: u64 = 0;

    timer.reset();
    for (0..num_ops) |i| {
        var buffer: [64]u8 = undefined;
        const key = std.fmt.bufPrint(buffer[0..32], "key_{d}", .{i}) catch unreachable;
        const kv = lsm.read(db, key) catch |err| {
            std.log.debug("database read error for key {s} {s}\n", .{
                key,
                @errorName(err),
            });
            error_count += 1;
            continue;
        };

        if (kv) |_| {
            success_count += 1;
        } else {
            error_count += 1;
        }
    }

    read_time = timer.read();

    const read_ops_per_sec = @as(f64, @floatFromInt(num_ops)) / (@as(f64, @floatFromInt(read_time)) / std.time.ns_per_s);

    std.log.info("Read Benchmark Results:", .{});
    std.log.info("  Total: {d} Errors: {d} Read: {d:.2} ops/sec ({d:.2} ms total)", .{
        success_count, error_count, read_ops_per_sec, @as(f64, @floatFromInt(read_time)) / std.time.ns_per_ms,
    });
}
