const std = @import("std");
const lsm = @import("lsm");

const msg = struct {
    key: []const u8,
    value: []const u8,
};

pub fn main() !void {
    var mainTimer = try std.time.Timer.start();
    const mainStart = mainTimer.read();

    var garena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer garena.deinit();

    const galloc = garena.allocator();
    var mailbox = try lsm.MessageQueue([]msg).init(galloc, ".");
    defer mailbox.deinit();

    const reader = try std.Thread.spawn(.{}, struct {
        pub fn consume(inbox: lsm.MessageQueue([]msg).ReadIter) void {
            var timer = std.time.Timer.start() catch |err| {
                std.debug.print("consume error {s}\n", .{@errorName(err)});
                return;
            };

            var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer arena.deinit();

            const alloc = arena.allocator();
            var db = lsm.defaultDatabase(alloc) catch |err| {
                std.debug.print("consume error {s}\n", .{@errorName(err)});
                return;
            };
            defer db.deinit();

            const start = timer.read();
            var count: usize = 0;
            while (inbox.next()) |row| {
                for (row) |item| {
                    db.write(item.key, item.value) catch |err| {
                        std.debug.print("count {d} {s}\n", .{ count, @errorName(err) });
                        return;
                    };
                    count += 1;
                }
            }
            const end = timer.read();
            std.debug.print("total rows read {d} in {}ms\n", .{ count, (end - start) / 1_000_000 });
        }
    }.consume, .{mailbox.subscribe()});
    const writer = mailbox.publisher();

    const file = std.fs.cwd().openFile("data/input.csv", .{}) catch |err| {
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

    var out = std.ArrayList(msg).init(galloc);
    defer out.deinit();

    const writeStart = mainTimer.read();
    var count: usize = 0;
    var idx: usize = 0;
    var row = [2][]const u8{ undefined, undefined };
    while (csv_file.next() catch null) |token| {
        switch (token) {
            .field => |val| {
                row[idx] = val;
                idx += 1;
            },
            .row_end => {
                const r: msg = .{ .key = row[0], .value = row[1] };
                try out.append(r);
                if (out.items.len >= std.mem.page_size) {
                    writer.publish(out.items) catch |err| {
                        std.debug.print("error publishing row {s}\n", .{@errorName(err)});
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
            std.debug.print("error publishing row {s}\n", .{@errorName(err)});
            return;
        };
    }
    const writeEnd = mainTimer.read();
    std.debug.print("total rows writen {d} in {}ms\n", .{ count, (writeEnd - writeStart) / 1_000_000 });
    writer.done() catch |err| {
        std.debug.print("publish error {s}\n", .{@errorName(err)});
    };

    reader.join();

    const mainEnd = mainTimer.read();
    std.debug.print("reading data finished in {}ms\n", .{(mainEnd - mainStart) / 1_000_000});
}
