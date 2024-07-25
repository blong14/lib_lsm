const std = @import("std");
const lsm = @import("lsm");

const msg = struct {
    key: []const u8,
    value: []const u8,
};

pub fn main() !void {
    var mainTimer = try std.time.Timer.start();
    const mainStart = mainTimer.read();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const galloc = gpa.allocator();
    var mailbox = try lsm.MessageQueue([]msg).init(galloc, ".");
    defer galloc.destroy(mailbox);
    defer mailbox.deinit();

    const reader = try std.Thread.spawn(.{}, struct {
        pub fn consume(inbox: lsm.MessageQueue([]msg).ReadIter) void {
            var timer = std.time.Timer.start() catch |err| {
                std.debug.print("consume error {s}\n", .{@errorName(err)});
                return;
            };

            const alloc = std.heap.c_allocator;
            var db = lsm.defaultDatabase(alloc) catch |err| {
                std.debug.print("consume error {s}\n", .{@errorName(err)});
                return;
            };
            defer alloc.destroy(db);
            defer db.deinit();

            const start = timer.read();
            var count: usize = 0;
            while (inbox.next()) |row| {
                for (row) |item| {
                    db.write(item.key, item.value) catch |err| {
                        std.debug.print("count {d} key {s} {s}\n", .{ count, item.key, @errorName(err) });
                        return;
                    };
                    count += 1;
                }
            }
            const end = timer.read();
            std.debug.print("total rows consumed {d} in {}ms\n", .{ count, (end - start) / 1_000_000 });

            db.flush() catch |err| {
                std.debug.print("error flushing db {s}\n", .{@errorName(err)});
                return;
            };

            std.debug.print("iterating keys...\n", .{});
            var iter = db.iterator() catch |err| {
                std.debug.print("db iter error {s}\n", .{@errorName(err)});
                return;
            };
            defer iter.deinit();

            const readStart = timer.read();
            var readCount: usize = 0;
            while (iter.next() catch false) {
                readCount += 1;
            }
            const readEnd = timer.read();
            std.debug.print("total rows read {d} in {}ms\n", .{ readCount, (readEnd - readStart) / 1_000_000 });
        }
    }.consume, .{mailbox.subscribe()});
    const writer = mailbox.publisher();

    const file = std.fs.cwd().openFile("data/trips.txt", .{}) catch |err| {
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
