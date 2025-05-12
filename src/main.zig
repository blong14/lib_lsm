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
        debug.print("{s}\n", .{usage});
        return;
    }

    const default_opts = lsm.defaultOpts();

    const data_dir = res.args.data_dir orelse default_opts.data_dir;
    const sst_capacity = res.args.sst_capacity orelse default_opts.sst_capacity;
    const wal_capacity = default_opts.wal_capacity;

    const opts: lsm.Opts = .{
        .data_dir = data_dir,
        .sst_capacity = sst_capacity,
        .wal_capacity = wal_capacity,
        .num_levels = 3,
    };

    const db = lsm.databaseFromOpts(allocator, opts) catch |err| {
        std.log.err("database init error {s}", .{@errorName(err)});
        return err;
    };
    defer allocator.destroy(db);
    defer db.deinit();

    try db.open();

    var it = try db.iterator(allocator);
    defer it.deinit();

    var count: usize = 0;
    while (it.next()) |kv| {
        count += 1;
        std.log.info("{s}", .{kv});
    }

    std.log.info("total rows {d}", .{count});
}
