const std = @import("std");
const lsm = @import("lsm");

pub fn main() !void {
    var mainTimer = try std.time.Timer.start();
    const mainStart = mainTimer.read();

    const msg = struct {
        key: []const u8,
        value: []const u8,
    };

    var mailbox = try lsm.MessageQueue(msg).init(".");
    defer mailbox.deinit();

    const writer = try std.Thread.spawn(.{}, struct {
        pub fn publish(outbox: lsm.MessageQueue(msg).Writer) void {
            defer outbox.done() catch |err| {
                std.debug.print("publish error {s}\n", .{@errorName(err)});
            };

            var timer = std.time.Timer.start() catch |err| {
                std.debug.print("publish error {s}\n", .{@errorName(err)});
                return;
            };
            const start = timer.read();

            const file = std.fs.cwd().openFile("data/input.csv", .{}) catch |err| {
                std.debug.print("publish error {s}\n", .{@errorName(err)});
                return;
            };
            defer file.close();

            // TODO: Figure out an appropriate buffer size
            var buffer = [_]u8{0} ** (4096 * 2000);
            const reader = file.reader();
            var csv_file = lsm.CSV.init(reader, &buffer, .{}) catch |err| {
                std.debug.print("publish error {s}\n", .{@errorName(err)});
                return;
            };

            const key: usize = 0;
            const value: usize = 1;
            var count: usize = 0;
            var idx: usize = 0;
            var row = [_][]const u8{undefined} ** 2;
            while (csv_file.next() catch null) |token| {
                switch (token) {
                    .field => |val| {
                        row[idx] = val;
                        idx += 1;
                    },
                    .row_end => {
                        const m: msg = .{ .key = row[key], .value = row[value] };
                        outbox.publish(m) catch |err| {
                            std.debug.print("error publishing row {s}\n", .{@errorName(err)});
                            break;
                        };
                        row = [_][]const u8{undefined} ** 2;
                        idx = 0;
                        count += 1;
                    },
                }
            }

            const end = timer.read();
            std.debug.print("total rows writen {d} in {}ms\n", .{ count, (end - start) / 1_000_000 });
        }
    }.publish, .{mailbox.publisher()});

    const reader = try std.Thread.spawn(.{}, struct {
        pub fn consume(inbox: lsm.MessageQueue(msg).ReadIter) void {
            var timer = std.time.Timer.start() catch |err| {
                std.debug.print("consume error {s}\n", .{@errorName(err)});
                return;
            };
            const start = timer.read();

            var gpa = std.heap.GeneralPurposeAllocator(.{}){};
            defer _ = gpa.deinit();

            const alloc = gpa.allocator();
            var db = lsm.defaultDatabase(alloc) catch |err| {
                std.debug.print("consume error {s}\n", .{@errorName(err)});
                return;
            };
            defer db.deinit();
            errdefer db.deinit();

            var count: usize = 0;
            while (inbox.next()) |row| {
                db.write(row.key, row.value) catch |err| {
                    std.debug.print("count {d} {s}\n", .{ count, @errorName(err) });
                    return;
                };
                count += 1;
            }

            const end = timer.read();
            std.debug.print("total rows read {d} in {}ms\n", .{ count, (end - start) / 1_000_000 });
        }
    }.consume, .{mailbox.subscribe()});

    writer.join();
    reader.join();

    const mainEnd = mainTimer.read();
    std.debug.print("reading data finished in {}ms\n", .{(mainEnd - mainStart) / 1_000_000});
}
