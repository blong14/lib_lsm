const std = @import("std");

const clap = @import("clap");
const lsm = @import("lsm");

const debug = std.debug;
const io = std.io;

const msg = struct {
    key: []const u8,
    value: []const u8,
};

pub fn run(data_dir: []const u8, input: []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const galloc = gpa.allocator();

    var mailbox = try lsm.MessageQueue([]msg).init(galloc, ".");
    defer galloc.destroy(mailbox);
    defer mailbox.deinit();

    const reader = try std.Thread.spawn(.{}, struct {
        pub fn consume(dir: []const u8, inbox: lsm.MessageQueue([]msg).ReadIter) void {
            const alloc = std.heap.c_allocator;

            lsm.BeginProfile(alloc);
            defer lsm.EndProfile();

            const db = lsm.databaseFromOpts(alloc, .{
                .data_dir = dir,
                .sst_capacity = 100_000,
                .wal_capacity = std.mem.page_size * std.mem.page_size,
            }) catch |err| {
                debug.print(
                    "database init error {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
            defer alloc.destroy(db);
            defer db.deinit();

            var timer = lsm.BlockProfiler.start("reader");
            var count: usize = 0;
            while (inbox.next()) |row| {
                for (row) |item| {
                    db.write(item.key, item.value) catch |err| {
                        std.debug.print(
                            "db write error count {d} key {s} {s}\n",
                            .{ count, item.key, @errorName(err) },
                        );
                        return;
                    };
                    count += 1;
                }
            }
            timer.end();

            db.flush() catch |err| {
                std.debug.print(
                    "error flushing db {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
        }
    }.consume, .{ data_dir, mailbox.subscribe() });

    const writer = mailbox.publisher();

    const file = std.fs.cwd().openFile(input, .{}) catch |err| {
        std.debug.print(
            "not able to open file {s}\n",
            .{@errorName(err)},
        );
        return;
    };
    defer file.close();

    const fileReader = file.reader();
    var buffer = [_]u8{0} ** std.mem.page_size;
    var csv_file = lsm.CSV.init(fileReader, &buffer, .{}) catch |err| {
        std.debug.print("publish error {s}\n", .{@errorName(err)});
        return;
    };

    var out = try std.ArrayList(msg).initCapacity(galloc, std.mem.page_size);
    defer out.deinit();

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
                const r: msg = .{ .key = row[0], .value = row[1] };
                try out.append(r);
                if (out.items.len >= std.mem.page_size) {
                    writer.publish(out.items) catch |err| {
                        std.debug.print(
                            "error publishing row {s}\n",
                            .{@errorName(err)},
                        );
                        return;
                    };
                    out.clearRetainingCapacity();
                }
                count += 1;
                row = [2][]const u8{ undefined, undefined };
                idx = 0;
            },
        }
    }

    if (out.items.len > 0) {
        writer.publish(out.items) catch |err| {
            std.debug.print(
                "error publishing row {s}\n",
                .{@errorName(err)},
            );
            return;
        };
    }

    try writer.done();
    reader.join();
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
    const input = res.args.input orelse "trips.txt";

    try run(data_dir, input);
}
