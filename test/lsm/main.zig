const std = @import("std");

const clap = @import("clap");
const lsm = @import("lsm");

const debug = std.debug;
const fs = std.fs;
const heap = std.heap;
const io = std.io;
const mem = std.mem;

const Allocator = mem.Allocator;

pub fn main() !void {
    var gpa = heap.GeneralPurposeAllocator(.{}){};
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
    const sst_capacity = res.args.sst_capacity orelse 300_000;
    const wal_capacity = mem.page_size * mem.page_size;
    const alloc = heap.c_allocator;

    const db = lsm.databaseFromOpts(alloc, .{
        .data_dir = data_dir,
        .sst_capacity = sst_capacity,
        .wal_capacity = wal_capacity,
    }) catch |err| {
        debug.print("database init error {s}\n", .{@errorName(err)});
        return err;
    };
    defer alloc.destroy(db);
    defer db.deinit();

    lsm.BeginProfile(gpa.allocator());
    defer lsm.EndProfile();

    const rows = try parse(gpa.allocator(), input);
    defer rows.deinit();

    write(db, rows);

    read(db);
}

pub fn parse(alloc: Allocator, input: []const u8) !std.ArrayList([2][]const u8) {
    const file = fs.cwd().openFile(input, .{}) catch |err| {
        debug.print("open file error {s}\n", .{@errorName(err)});
        return err;
    };
    defer file.close();

    // TODO: Figure out an appropriate buffer size
    var buffer = [_]u8{0} ** (mem.page_size * 2000);
    const fileReader = file.reader();
    var csv_file = lsm.CSV.init(fileReader, &buffer, .{}) catch |err| {
        debug.print("csv file error {s}\n", .{@errorName(err)});
        return err;
    };

    var count: usize = 0;
    var idx: usize = 0;
    var row = [2][]const u8{ undefined, undefined };

    var out = try std.ArrayList([2][]const u8).initCapacity(alloc, 500_000);

    while (csv_file.next() catch |err| {
        debug.print(
            "not able to read next token {s}\n",
            .{@errorName(err)},
        );
        return err;
    }) |token| {
        switch (token) {
            .field => |val| {
                if (idx < row.len) {
                    row[idx] = val;
                    idx += 1;
                }
            },
            .row_end => {
                try out.append(row);
                count += 1;
                row = [2][]const u8{ undefined, undefined };
                idx = 0;
            },
        }
    }
    return out;
}

pub fn write(db: *lsm.Database, input: std.ArrayList([2][]const u8)) void {
    var timer = lsm.BlockProfiler.start("write");
    defer timer.end();

    var bytes: u64 = 0;
    for (input.items) |row| {
        db.write(row[0], row[1]) catch |err| {
            debug.print(
                "database write error: key {s} error {s}\n",
                .{ row[0], @errorName(err) },
            );
            return;
        };
        bytes += row[0].len + row[1].len;
    }

    timer.withBytes(bytes);
}

pub fn read(db: *lsm.Database) void {
    var iter = db.iterator() catch |err| {
        debug.print(
            "database iter err: {s}\n",
            .{@errorName(err)},
        );
        return;
    };
    defer iter.deinit();

    var count: usize = 0;
    while (iter.next() catch null) {
        count += 1;
    }
}
