const std = @import("std");
const lsm = @import("lsm");
const zzmq = @import("zzmq");

const FixedBuffer = std.io.FixedBufferStream([]u8);

const Msg = extern struct {
    key: [*c]const u8,
    value: [*c]const u8,
};

var stopRunning_ = std.atomic.Value(bool).init(false);
const stopRunning = &stopRunning_;

fn sig_handler(_: c_int) align(1) callconv(.C) void {
    stopRunning.store(true, .seq_cst);
}

const sig_ign = std.os.linux.Sigaction{
    .handler = .{ .handler = &sig_handler },
    .mask = std.os.linux.empty_sigset,
    .flags = 0,
};

pub fn main() !void {
    var mainTimer = try std.time.Timer.start();
    const mainStart = mainTimer.read();

    const reader = try std.Thread.spawn(.{}, struct {
        pub fn consume() void {
            std.debug.print("starting consumer...\n", .{});

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

            var context = zzmq.ZContext.init(alloc) catch |err| {
                std.debug.print("context error {s}\n", .{@errorName(err)});
                return;
            };
            defer context.deinit();

            var socket = zzmq.ZSocket.init(zzmq.ZSocketType.Pair, &context) catch |err| {
                std.debug.print("socket init error {s}\n", .{@errorName(err)});
                return;
            };
            defer socket.deinit();

            std.debug.print("connecting to socket...\n", .{});
            socket.bind("tcp://127.0.0.1:5555") catch |err| {
                std.debug.print("not able to connect {s}\n", .{@errorName(err)});
                return;
            };

            const start = timer.read();
            var count: usize = 0;
            while (!stopRunning.load(.seq_cst)) {
                var msg = socket.receive(.{ .dontwait = true }) catch |err| switch (err) {
                    error.NonBlockingQueueEmpty => continue,
                    else => {
                        std.debug.print("socket recv error: {s} {}\n", .{ @errorName(err), err });
                        return;
                    },
                };
                const data = msg.data() catch |err| {
                    std.debug.print("invalid msg data: {s} {}\n", .{ @errorName(err), err });
                    continue;
                };
                var buf: FixedBuffer = std.io.fixedBufferStream(@constCast(data));
                while (true) {
                    const r = buf.reader().readStruct(Msg) catch |err| {
                        std.debug.print("invalid MSG data: {s} {}\n", .{ @errorName(err), err });
                        break;
                    };
                    std.debug.print("{s}\n\n", .{r.value});
                    // const key = std.mem.span(r.key);
                    // const value = std.mem.span(r.value);
                    // std.debug.print("{d} key {d} value {d} - diff {d}\n", .{ count, key.len, value.len, (key.len - value.len) });
                    //                   db.write(key, value) catch |err| {
                    //                      std.debug.print("count {d} {s}\n", .{ count, @errorName(err) });
                    //                       msg.deinit();
                    //                      return;
                    //                 };
                    count += 1;
                }
                msg.deinit();
            }

            const end = timer.read();
            std.debug.print("total rows read {d} in {}ms\n", .{ count, (end - start) / 1_000_000 });
        }
    }.consume, .{});

    var garena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer garena.deinit();

    const galloc = garena.allocator();

    var context = try zzmq.ZContext.init(galloc);
    defer context.deinit();

    var socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Pair, &context);
    defer socket.deinit();

    _ = std.os.linux.sigaction(std.os.linux.SIG.HUP, &sig_ign, null);
    _ = std.os.linux.sigaction(std.os.linux.SIG.INT, &sig_ign, null);
    _ = std.os.linux.sigaction(std.os.linux.SIG.QUIT, &sig_ign, null);
    _ = std.os.linux.sigaction(std.os.linux.SIG.TERM, &sig_ign, null);

    try socket.connect("tcp://127.0.0.1:5555");

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

    const outSize = @sizeOf(Msg) * std.mem.page_size;
    const out = try galloc.alloc(u8, outSize);
    defer galloc.free(out);

    var buf: FixedBuffer = std.io.fixedBufferStream(out);

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
                const r: Msg = .{ .key = row[0].ptr, .value = row[1].ptr };
                try buf.writer().writeStruct(r);
                if (buf.getWritten().len >= outSize) {
                    var msg = try zzmq.ZMessage.init(galloc, buf.getWritten());
                    defer msg.deinit();
                    std.debug.print("sending on socket...\n", .{});
                    socket.send(&msg, .{
                        .more = false,
                        .dontwait = false,
                    }) catch |err| {
                        std.debug.print("error publishing row {s}\n", .{@errorName(err)});
                        return;
                    };
                    buf.reset();
                }
                count += 1;
                row = [2][]const u8{ undefined, undefined };
                idx = 0;
            },
        }
    }
    if ((buf.getEndPos() catch 0) > 0) {
        var msg = try zzmq.ZMessage.init(galloc, buf.getWritten());
        defer msg.deinit();
        socket.send(&msg, .{}) catch |err| {
            std.debug.print("error publishing row {s}\n", .{@errorName(err)});
            return;
        };
        buf.reset();
    }
    const writeEnd = mainTimer.read();
    std.debug.print("total rows writen {d} in {}ms\n", .{ count, (writeEnd - writeStart) / 1_000_000 });

    stopRunning.store(true, .seq_cst);
    reader.join();

    const mainEnd = mainTimer.read();
    std.debug.print("reading data finished in {}ms\n", .{(mainEnd - mainStart) / 1_000_000});
}
