const std = @import("std");
const lsm = @import("lsm");

const aalloc = std.heap.ArenaAllocator{};

pub fn main() !void {
    var timer = try std.time.Timer.start();
    const start = timer.read();
    const file = try std.fs.cwd().openFile("data/input.csv", .{});
    defer file.close();

    var buffer = [_]u8{0} ** (4096 * 4096);
    const reader = file.reader();
    var csv_file = try lsm.CSV.init(reader, &buffer, .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();
    var db = try lsm.defaultDatabase(alloc);

    const key: usize = 0;
    const value: usize = 1;
    var count: usize = 0;
    var idx: usize = 0;
    var row = [_][]const u8{undefined} ** 2;
    while (try csv_file.next()) |token| {
        switch (token) {
            .field => |val| {
                row[idx] = val;
                idx += 1;
            },
            .row_end => {
                db.write(row[key], row[value]) catch |err| {
                    std.debug.print("count {d} {s}\n", .{ count, @errorName(err) });
                    return;
                };
                count += 1;
                row = [_][]const u8{undefined} ** 2;
                idx = 0;
            },
        }
    }

    const end = timer.read();
    std.debug.print("total rows read {d} in {}ms\n", .{ count, (end - start) / 1_000_000 });
    db.deinit();
    _ = gpa.deinit();
}
