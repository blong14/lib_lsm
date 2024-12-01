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
    }

    fn parse(alloc: Allocator, input: []const u8) !void {
        const file = fs.cwd().openFile(input, .{}) catch |err| {
            debug.print("open file error {s}\n", .{@errorName(err)});
            return err;
        };
        defer file.close();

        var buffer = [_]u8{0} ** mem.page_size;
        const fileReader = file.reader();
        var csv_file = lsm.CSV.init(fileReader, &buffer, .{ .col_sep = ';' }) catch |err| {
            debug.print("csv file error {s}\n", .{@errorName(err)});
            return err;
        };

        var idx: usize = 0;
        var row = [2][]const u8{ undefined, undefined };
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
                    }
                    idx += 1;
                },
                .row_end => {
                    const key = try alloc.alloc(u8, row[0].len);
                    mem.copyForwards(u8, key, row[0]);

                    const value = try alloc.alloc(u8, row[1].len);
                    mem.copyForwards(u8, value, row[1]);

                    self.db.write(key, value) catch |err| {
                        debug.print(
                            "database write error: key {s} value {s} error {s}\n",
                            .{ key, value, @errorName(err) },
                        );
                        return;
                    };

                    idx = 0;
                    row = [2][]const u8{ undefined, undefined };
                },
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

    fn read() void {
        var iter = self.db.iterator() catch |err| {
            debug.print(
                "database iter err: {s}\n",
                .{@errorName(err)},
            );
            return;
        };
        defer iter.deinit();

        var count: usize = 0;
        while (iter.next() catch null) {
            debug.print(
                "key {s} value {s}\n",
                .{ iter.value().key, iter.value().value },
            );
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
            const file = fs.cwd().openFile(input, .{}) catch |err| {
                debug.print(
                    "not able to open file {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
            defer file.close();

            const fileReader = file.reader();
            var buffer = [_]u8{0} ** mem.page_size;
            var csv_file = lsm.CSV.init(fileReader, &buffer, .{ .col_sep = ';' }) catch |err| {
                debug.print("publish error {s}\n", .{@errorName(err)});
                return;
            };

            var kvs = std.ArrayList([]const u8).init(alloc);
            defer kvs.deinit();

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
                        const item = KV.init(row[0], row[1]);

                        const data = item.encodeAlloc(alloc) catch return;
                        kvs.append(data) catch |e| {
                            debug.print("{s}\n", .{@errorName(e)});
                            return;
                        };

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

                        count += 1;
                        row = [2][]const u8{ undefined, undefined };
                        idx = 0;
                    },
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

        var mailbox = try lsm.MessageQueue([]const u8).init(allocator, ".");
        defer mailbox.deinit();

        const reader = try std.Thread.spawn(.{}, struct {
            pub fn consume(opts: lsm.Opts, inbox: *const lsm.MessageQueue([]const u8).ReadIter) void {
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

        const file = fs.cwd().openFile(input, .{}) catch |err| {
            debug.print(
                "not able to open file {s}\n",
                .{@errorName(err)},
            );
            return;
        };
        defer file.close();

        const fileReader = file.reader();
        var buffer = [_]u8{0} ** mem.page_size;
        var csv_file = lsm.CSV.init(fileReader, &buffer, .{ .col_sep = ';' }) catch |err| {
            debug.print("publish error {s}\n", .{@errorName(err)});
            return;
        };

        var out = std.ArrayList([]const u8).init(allocator);
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
                    const item = KV.init(row[0], row[1]);

                    const data = item.encodeAlloc(allocator) catch return;
                    out.append(data) catch |err| {
                        debug.print(
                            "{s}\n",
                            .{@errorName(err)},
                        );
                        return;
                    };

                    if (out.items.len >= mem.page_size) {
                        for (out.items) |i| {
                            const msg = i;
                            writer.publish(msg) catch |err| {
                                debug.print(
                                    "error publishing row {s}\n",
                                    .{@errorName(err)},
                                );
                                return;
                            };
                        }
                        out.clearRetainingCapacity();
                    }
                    count += 1;
                    row = [2][]const u8{ undefined, undefined };
                    idx = 0;
                },
            }
        }

        if (out.items.len > 0) {
            for (out.items) |i| {
                const msg = i;
                writer.publish(msg) catch |err| {
                    debug.print(
                        "error publishing row {s}\n",
                        .{@errorName(err)},
                    );
                    return;
                };
            }
        }

        try writer.done();
        reader.join();
    }
};
