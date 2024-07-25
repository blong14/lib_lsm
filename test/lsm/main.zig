const std = @import("std");

const clap = @import("clap");
const lsm = @import("lsm");

const debug = std.debug;
const io = std.io;

pub fn xmain(data_dir: []const u8, input: []const u8) !void {
    var mainTimer = try std.time.Timer.start();
    const mainStart = mainTimer.read();

    const alloc = std.heap.c_allocator;

    const file = std.fs.cwd().openFile(input, .{}) catch |err| {
        std.debug.print("publish error {s}\n", .{@errorName(err)});
        return;
    };
    defer file.close();

    // TODO: Figure out an appropriate buffer size
    var buffer = [_]u8{0} ** (std.mem.page_size * 2000);
    const fileReader = file.reader();
    var csv_file = lsm.CSV.init(fileReader, &buffer, .{}) catch |err| {
        std.debug.print("publish error {s}\n", .{@errorName(err)});
        return;
    };

    const opts = lsm.withDataDirOpts(data_dir);
    const db = lsm.databaseFromOpts(alloc, opts) catch |err| {
        std.debug.print("consume error {s}\n", .{@errorName(err)});
        return;
    };
    defer alloc.destroy(db);
    defer db.deinit();

    const writeStart = mainTimer.read();
    var count: usize = 0;
    var idx: usize = 0;
    var row = [2][]const u8{ undefined, undefined };
    while (csv_file.next() catch null) |token| {
        switch (token) {
            .field => |val| {
                if (idx < row.len) {
                    row[idx] = val;
                    idx += 1;
                }
            },
            .row_end => {
                db.write(row[0], row[1]) catch |err| {
                    std.debug.print("count {d} key {s} {s}\n", .{ count, row[0], @errorName(err) });
                    return;
                };
                count += 1;
                row = [2][]const u8{ undefined, undefined };
                idx = 0;
            },
        }
    }

    const writeEnd = mainTimer.read();
    std.debug.print("total rows written {d} in {}ms\n", .{ count, (writeEnd - writeStart) / 1_000_000 });

    db.flush() catch |err| {
        std.debug.print("error flushing db {s}\n", .{@errorName(err)});
        return;
    };

    const consumeEnd = mainTimer.read();
    std.debug.print("total rows consumed {d} in {}ms\n", .{ count, (consumeEnd - writeStart) / 1_000_000 });

    std.debug.print("iterating keys...\n", .{});
    var iter = db.iterator() catch |err| {
        std.debug.print("db iter error {s}\n", .{@errorName(err)});
        return;
    };
    defer iter.deinit();

    const readStart = mainTimer.read();
    var readCount: usize = 0;
    while (iter.next() catch false) {
        readCount += 1;
    }
    const readEnd = mainTimer.read();
    std.debug.print("total rows read {d} in {}ms\n", .{ readCount, (readEnd - readStart) / 1_000_000 });

    const mainEnd = mainTimer.read();
    std.debug.print("reading data finished in {}ms\n", .{(mainEnd - mainStart) / 1_000_000});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`
    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-d, --data_dir <str>   The data directory to save files on disk.
        \\-i, --input    <str>   An input file to import. Only supports csv.
        \\--sst_capacity <usize> Max capacity for a SST block.
        \\
    );

    // Initialize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = gpa.allocator(),
    }) catch |err| {
        // Report useful error and exit
        diag.report(io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        debug.print("--help\n", .{});
        return;
    }

    const data_dir = res.args.data_dir orelse "data";
    const input = res.args.input orelse "data/trips.txt";

    try xmain(data_dir, input);
}
