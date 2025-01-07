const std = @import("std");

const clap = @import("clap");
const lsm = @import("lsm");

const debug = std.debug;
const fs = std.fs;
const heap = std.heap;
const io = std.io;
const mem = std.mem;

const Allocator = mem.Allocator;

const KV = lsm.KV;

const usage =
    \\-h, --help             Display this help and exit.
    \\-d, --data_dir <str>   The data directory to save files on disk.
    \\-i, --input    <str>   An input file to import. Only supports csv.
    \\-m, --mode     <mode>  Execution mode. Can be one of singlethreaded, multithreaded, or multiprocess
    \\--sst_capacity <usize> Max capacity for an SST block.
    \\
;

pub fn main() !void {
    var gpa = heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`
    const params = comptime clap.parseParamsComptime(usage);

    const Mode = enum { singlethreaded, multithreaded, multiprocess };
    const parsers = comptime .{
        .str = clap.parsers.string,
        .usize = clap.parsers.int(usize, 10),
        .mode = clap.parsers.enumeration(Mode),
    };

    // Initialize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, parsers, .{
        .diagnostic = &diag,
        .allocator = gpa.allocator(),
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

    const input = res.args.input orelse "measurements.txt";
    const mode = res.args.mode orelse Mode.singlethreaded;

    const alloc = gpa.allocator();

    const opts: lsm.Opts = .{
        .data_dir = data_dir,
        .sst_capacity = sst_capacity,
        .wal_capacity = wal_capacity,
    };

    const impl: Runnable = switch (mode) {
        .singlethreaded => try SingleThreadedImpl.init(alloc, opts),
        .multithreaded => try MultiThreadedImpl.init(alloc, opts),
        .multiprocess => try MultiProcessImpl.init(alloc, opts),
    };

    try impl.run(input);
}

/// Runnable defines the standard interface for a command
const Runnable = struct {
    run: *const fn (input: []const u8) anyerror!void,
};

const SingleThreadedImpl = struct {
    alloc: Allocator,
    db: *lsm.Database,

    const Self = @This();

    var self: *Self = undefined;

    pub fn init(alloc: Allocator, opts: lsm.Opts) !Runnable {
        const db = lsm.databaseFromOpts(heap.c_allocator, opts) catch |err| {
            debug.print("database init error {s}\n", .{@errorName(err)});
            return err;
        };

        self = try alloc.create(Self);
        self.* = .{
            .alloc = alloc,
            .db = db,
        };
        return .{
            .run = Self.run,
        };
    }

    pub fn deinit(this: *Self) void {
        this.db.deinit();
        this.alloc.destroy(self);
    }

    pub fn run(input: []const u8) anyerror!void {
        defer self.deinit();

        var arena = heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();

        const allocator = arena.allocator();

        lsm.BeginProfile(allocator);
        defer lsm.EndProfile();

        try parse(allocator, input);

        // read(allocator);
    }

    var ballast: usize = std.mem.page_size;

    fn xalloc(alloc: Allocator, buffer: *[]u8, len_: usize) ![]u8 {
        if (buffer.*.len == 0) {
            buffer.* = try alloc.alloc(u8, ballast);
            ballast *= 2;
        }
        const result = @subWithOverflow(buffer.*.len - 1, len_);
        var offset = result[0];
        if (result[1] != 0) {
            buffer.* = try alloc.alloc(u8, ballast);
            ballast *= 2;
            offset = (buffer.*.len - 1) - len_;
        }
        var n: []u8 = undefined;
        n = buffer.*[offset .. buffer.*.len - 1];
        buffer.* = buffer.*[0..offset];
        return n;
    }

    test "xalloc" {
        const testing = std.testing;
        const alloc = testing.allocator;

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        var buffer = try arena.allocator().alloc(u8, ballast);

        var actual = try xalloc(arena.allocator(), &buffer, 4094);

        try testing.expectEqual(actual.len, 4094);

        actual = try xalloc(arena.allocator(), &buffer, 4094);

        try testing.expectEqual(actual.len, 4094);

        actual = try xalloc(arena.allocator(), &buffer, 2);

        try testing.expectEqual(actual.len, 2);
    }

    fn parse(alloc: Allocator, input: []const u8) !void {
        var idx: usize = 0;
        var data = [2][]const u8{ undefined, undefined };
        var byte_buffer = try alloc.alloc(u8, ballast);

        var cnt: usize = 0;
        const handle = lsm.CsvOpen2(input.ptr, ';', '"', '\\');
        while (lsm.ReadNextRow(handle)) |row| {
            while (lsm.ReadNextCol(row, handle)) |val| {
                if (idx < data.len) {
                    data[idx] = mem.span(val);
                }
                idx += 1;
                if (idx == 2) {
                    const key_len = data[0].len;
                    const key = try xalloc(alloc, &byte_buffer, key_len);
                    mem.copyForwards(u8, key, data[0]);

                    const value_len = data[1].len;
                    const value = try xalloc(alloc, &byte_buffer, value_len);
                    mem.copyForwards(u8, value, data[1]);

                    self.db.write(key, value) catch |err| {
                        debug.print(
                            "database write error: key {s} value {s} error {s}\n",
                            .{ data[0], data[1], @errorName(err) },
                        );
                        return;
                    };
                    idx = 0;
                    cnt += 1;
                }
            }
        }

        self.db.flush() catch |err| {
            debug.print(
                "not able to flush database error {s}\n",
                .{@errorName(err)},
            );
            return;
        };
    }

    fn read(alloc: Allocator) void {
        var iter = self.db.iterator() catch |err| {
            debug.print(
                "database iter err: {s}\n",
                .{@errorName(err)},
            );
            return;
        };
        defer iter.deinit(alloc);

        var count: usize = 0;
        while (iter.next() catch false) {
            debug.print("{d}:\t{}\n", .{ count, iter.value() });
            count += 1;
        }
    }
};

const MultiThreadedImpl = struct {
    alloc: Allocator,
    opts: lsm.Opts,

    const Self = @This();

    var self: *Self = undefined;

    pub fn init(alloc: Allocator, opts: lsm.Opts) !Runnable {
        self = try alloc.create(Self);
        self.* = .{
            .alloc = alloc,
            .opts = opts,
        };
        return .{
            .run = Self.run,
        };
    }

    pub fn deinit(this: *Self) void {
        this.alloc.destroy(self);
    }

    const Reader = struct {
        pub fn publish(
            alloc: Allocator,
            input: []const u8,
            outbox: *lsm.ThreadMessageQueue([]const u8).Writer,
        ) void {
            var kvs = std.ArrayList([]const u8).init(alloc);
            defer kvs.deinit();

            var idx: usize = 0;
            var data_row = [2][]const u8{ undefined, undefined };

            const handle = lsm.CsvOpen2(input.ptr, ';', '"', '\\');
            while (lsm.ReadNextRow(handle)) |row| {
                while (lsm.ReadNextCol(row, handle)) |val| {
                    if (idx < data_row.len) {
                        data_row[idx] = mem.span(val);
                    }

                    idx += 1;
                    if (idx == 2) {
                        const item = KV.init(data_row[0], data_row[1]);

                        const data = item.encodeAlloc(alloc) catch |err| {
                            debug.print(
                                "not able to encode KV for key {s} error {s}\n",
                                .{ item.key, @errorName(err) },
                            );
                            return;
                        };

                        kvs.append(data) catch return;

                        if (kvs.items.len >= mem.page_size) {
                            for (kvs.items) |d| {
                                const msg = d;
                                outbox.publish(msg) catch |err| {
                                    debug.print(
                                        "not able to publish items {s}\n",
                                        .{@errorName(err)},
                                    );
                                };
                            }
                            kvs.clearRetainingCapacity();
                        }

                        idx = 0;
                    }
                }
            }

            if (kvs.items.len > 0) {
                for (kvs.items) |d| {
                    const msg = d;
                    outbox.publish(msg) catch |err| {
                        debug.print(
                            "not able to publish items {s}\n",
                            .{@errorName(err)},
                        );
                    };
                }
            }

            outbox.done() catch return;
        }
    };

    const Writer = struct {
        pub fn consume(opts: lsm.Opts, inbox: *lsm.ThreadMessageQueue([]const u8).ReadIter) void {
            const db = lsm.databaseFromOpts(heap.c_allocator, opts) catch |err| {
                debug.print(
                    "database init error {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
            defer db.deinit();

            var count: usize = 0;
            while (inbox.next()) |item| {
                var kv: KV = .{ .key = undefined, .value = undefined };
                kv.decode(item) catch |err| {
                    debug.print(
                        "not able to decode kv {s} {any}\n",
                        .{ @errorName(err), item },
                    );
                    return;
                };

                db.write(kv.key, kv.value) catch |err| {
                    debug.print(
                        "db write error count {d} key {s} {s}\n",
                        .{ count, kv.key, @errorName(err) },
                    );
                    return;
                };
                count += 1;
            }

            db.flush() catch |err| {
                debug.print(
                    "error flushing db {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
        }
    };

    pub fn run(input: []const u8) !void {
        defer self.deinit();

        var arena = heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();

        const allocator = arena.allocator();

        lsm.BeginProfile(allocator);
        defer lsm.EndProfile();

        var mailbox = try lsm.ThreadMessageQueue([]const u8).init(self.alloc);
        defer self.alloc.destroy(mailbox);
        defer mailbox.deinit();

        const publisher = try std.Thread.spawn(.{}, Reader.publish, .{
            allocator,
            input,
            @constCast(&mailbox.publisher()),
        });

        var reader = mailbox.subscribe();
        defer reader.deinit();

        const consumer = try std.Thread.spawn(.{}, Writer.consume, .{ self.opts, &reader });

        consumer.join();
        publisher.join();
    }
};

const MultiProcessImpl = struct {
    alloc: Allocator,
    opts: lsm.Opts,

    const Self = @This();

    var self: *Self = undefined;

    pub fn init(alloc: Allocator, opts: lsm.Opts) !Runnable {
        self = try alloc.create(Self);
        self.* = .{ .alloc = alloc, .opts = opts };
        return .{
            .run = Self.run,
        };
    }

    pub fn deinit(this: *Self) void {
        this.alloc.destroy(self);
    }

    pub fn run(input: []const u8) !void {
        defer self.deinit();

        var arena = heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();

        const allocator = arena.allocator();

        var mailbox = try lsm.ProcessMessageQueue.init(allocator, ".");
        defer mailbox.deinit();

        const reader = try std.Thread.spawn(.{}, struct {
            pub fn consume(opts: lsm.Opts, inbox: *const lsm.ProcessMessageQueue.ReadIter) void {
                const db = lsm.databaseFromOpts(heap.c_allocator, opts) catch |err| {
                    debug.print(
                        "database init error {s}\n",
                        .{@errorName(err)},
                    );
                    return;
                };
                defer db.deinit();

                var count: usize = 0;
                while (inbox.next()) |item| {
                    var kv: KV = .{ .key = undefined, .value = undefined };
                    kv.decode(item) catch |err| {
                        debug.print(
                            "not able to decode kv {s} {any}\n",
                            .{ @errorName(err), item },
                        );
                        return;
                    };

                    db.write(kv.key, kv.value) catch |err| {
                        debug.print(
                            "db write error count {d} key {s} {s}\n",
                            .{ count, kv.key, @errorName(err) },
                        );
                        return;
                    };
                    count += 1;
                }

                db.flush() catch |err| {
                    debug.print(
                        "error flushing db {s}\n",
                        .{@errorName(err)},
                    );
                    return;
                };
            }
        }.consume, .{ self.opts, &mailbox.subscribe() });

        const writer = mailbox.publisher();

        var kvs = std.ArrayList([]const u8).init(allocator);
        defer kvs.deinit();

        var idx: usize = 0;
        var data_row = [2][]const u8{ undefined, undefined };

        const handle = lsm.CsvOpen2(input.ptr, ';', '"', '\\');
        while (lsm.ReadNextRow(handle)) |row| {
            while (lsm.ReadNextCol(row, handle)) |val| {
                if (idx < data_row.len) {
                    data_row[idx] = mem.span(val);
                }

                idx += 1;
                if (idx == 2) {
                    const item = KV.init(data_row[0], data_row[1]);

                    const data = item.encodeAlloc(allocator) catch |err| {
                        debug.print(
                            "not able to encode KV for key {s} error {s}\n",
                            .{ item.key, @errorName(err) },
                        );
                        return;
                    };

                    kvs.append(data) catch return;

                    if (kvs.items.len >= mem.page_size) {
                        for (kvs.items) |d| {
                            const msg = d;
                            writer.publish(msg) catch |err| {
                                debug.print(
                                    "not able to publish items {s}\n",
                                    .{@errorName(err)},
                                );
                            };
                        }
                        kvs.clearRetainingCapacity();
                    }

                    idx = 0;
                }
            }
        }

        if (kvs.items.len > 0) {
            for (kvs.items) |d| {
                const msg = d;
                writer.publish(msg) catch |err| {
                    debug.print(
                        "not able to publish items {s}\n",
                        .{@errorName(err)},
                    );
                };
            }
        }

        try writer.done();
        reader.join();
    }
};
