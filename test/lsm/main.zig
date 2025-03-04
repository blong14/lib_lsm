const std = @import("std");

const clap = @import("clap");
const jemalloc = @import("jemalloc");
const lsm = @import("lsm");

const atomic = std.atomic;
const debug = std.debug;
const fs = std.fs;
const heap = std.heap;
const io = std.io;
const mem = std.mem;

const Allocator = mem.Allocator;

const KV = lsm.KV;
const ThreadSafeBumpAllocator = lsm.ThreadSafeBumpAllocator;

const allocator = jemalloc.allocator;
const usage =
    \\-h, --help             Display this help and exit.
    \\-d, --data_dir <str>   The data directory to save files on disk.
    \\-i, --input    <str>   An input file to import. Only supports csv.
    \\-m, --mode     <mode>  Execution mode. Can be one of singlethreaded, multithreaded, or multiprocess
    \\--sst_capacity <usize> Max capacity for an SST block.
    \\
;

pub fn main() !void {
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
        .allocator = allocator,
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

    const opts: lsm.Opts = .{
        .data_dir = data_dir,
        .sst_capacity = sst_capacity,
        .wal_capacity = wal_capacity,
    };

    const impl: Runnable = switch (mode) {
        .singlethreaded => try SingleThreadedImpl.init(),
        .multithreaded => try MultiThreadedImpl.init(),
        .multiprocess => try MultiProcessImpl.init(),
    };

    try impl.run(input, opts);
}

/// Runnable defines the standard interface for a command
const Runnable = struct {
    run: *const fn (input: []const u8, opts: lsm.Opts) anyerror!void,
};

const SingleThreadedImpl = struct {
    const Self = @This();

    pub fn init() !Runnable {
        return .{
            .run = Self.run,
        };
    }

    pub fn run(input: []const u8, opts: lsm.Opts) anyerror!void {
        // var arena = heap.ArenaAllocator.init(allocator);
        // defer arena.deinit();

        var arena = ThreadSafeBumpAllocator.init(allocator, std.mem.page_size * 2) catch return;
        defer arena.deinit();
        defer arena.printStats();

        const alloc = arena.allocator();

        lsm.BeginProfile(alloc);
        defer lsm.EndProfile();

        try parse(alloc, input, opts);
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

    fn parse(alloc: Allocator, input: []const u8, opts: lsm.Opts) !void {
        const db = lsm.databaseFromOpts(alloc, opts) catch |err| {
            debug.print("database init error {s}\n", .{@errorName(err)});
            return err;
        };
        var idx: usize = 0;
        var data = [2][]const u8{ undefined, undefined };
        // var byte_buffer = try alloc.alloc(u8, ballast);

        const handle = lsm.CsvOpen2(input.ptr, ';', '"', '\\');
        defer lsm.CsvClose(handle);

        var cnt: usize = 0;
        while (lsm.ReadNextRow(handle)) |row| {
            while (lsm.ReadNextCol(row, handle)) |val| {
                if (idx < data.len) {
                    data[idx] = mem.span(val);
                }
                idx += 1;
                if (idx == 2) {
                    const key_len = data[0].len;
                    const value_len = data[1].len;

                    // const key = try xalloc(alloc, &byte_buffer, key_len);
                    const byts = try alloc.alloc(u8, key_len + value_len);

                    mem.copyForwards(u8, byts[0..key_len], data[0]);
                    mem.copyForwards(u8, byts[key_len..], data[1]);

                    // const value = try xalloc(alloc, &byte_buffer, value_len);
                    // const value = try alloc.alloc(u8, value_len);

                    db.write(byts[0..key_len], byts[key_len..]) catch |err| {
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

        db.flush() catch |err| {
            debug.print(
                "not able to flush database error {s}\n",
                .{@errorName(err)},
            );
            return;
        };
    }
};

const MessageQueue = lsm.ThreadMessageQueue([][]const u8);

const MultiThreadedImpl = struct {
    const Self = @This();

    pub fn init() !Runnable {
        return .{
            .run = Self.run,
        };
    }

    var mtx: std.Thread.Mutex = .{};
    var signal: std.Thread.Condition = .{};
    var done: bool = false;

    const Reader = struct {
        pub fn publish(
            input: []const u8,
            outbox: *MessageQueue,
        ) void {
            var kvs = std.ArrayList([]const u8).init(allocator);
            defer kvs.deinit();

            var idx: usize = 0;
            var data_row = [2][]const u8{ undefined, undefined };

            const handle = lsm.CsvOpen2(input.ptr, ';', '"', '\\');
            defer lsm.CsvClose(handle);

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
                            const msg = kvs.toOwnedSlice() catch |err| {
                                debug.print(
                                    "not able to publish items {s}\n",
                                    .{@errorName(err)},
                                );
                                return;
                            };

                            const node = allocator.create(MessageQueue.Node) catch |err| {
                                debug.print(
                                    "not able to publish items {s}\n",
                                    .{@errorName(err)},
                                );
                                return;
                            };
                            node.* = .{
                                .prev = undefined,
                                .next = undefined,
                                .data = msg,
                            };

                            mtx.lock();
                            defer mtx.unlock();

                            outbox.put(node);

                            signal.broadcast();
                        }

                        idx = 0;
                    }
                }
            }

            if (kvs.items.len > 0) {
                const msg = kvs.toOwnedSlice() catch |err| {
                    debug.print(
                        "not able to publish items {s}\n",
                        .{@errorName(err)},
                    );
                    return;
                };

                const node = allocator.create(MessageQueue.Node) catch |err| {
                    debug.print(
                        "not able to publish items {s}\n",
                        .{@errorName(err)},
                    );
                    return;
                };
                node.* = .{
                    .prev = undefined,
                    .next = undefined,
                    .data = msg,
                };

                outbox.put(node);
            }

            {
                // Notify writer thread that we are finished publishing
                mtx.lock();
                defer mtx.unlock();

                done = true;
                signal.broadcast();
            }
        }
    };

    const Writer = struct {
        fn write(wg: *std.Thread.WaitGroup, db: *lsm.Database, items: [][]const u8) void {
            wg.start();
            defer wg.finish();

            for (items) |item| {
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
                        "db write error key {s} {s}\n",
                        .{ kv.key, @errorName(err) },
                    );
                    return;
                };
            }
        }

        pub fn consume(opts: lsm.Opts, inbox: *MessageQueue) void {
            var arena = ThreadSafeBumpAllocator.init(allocator, std.mem.page_size * 4) catch return;
            defer arena.deinit();
            defer arena.printStats();

            const arena_allocator = arena.allocator();
            const db = lsm.databaseFromOpts(arena_allocator, opts) catch |err| {
                debug.print(
                    "database init error {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
            defer db.deinit();

            const Pool = std.Thread.Pool;

            var thread_pool: Pool = undefined;
            thread_pool.init(Pool.Options{
                .allocator = allocator,
            }) catch |err| {
                debug.print(
                    "threadpool init error {s}\n",
                    .{@errorName(err)},
                );
            };
            defer thread_pool.deinit();

            var wait_group: std.Thread.WaitGroup = undefined;
            wait_group.reset();

            var count: usize = 0;
            while (true) {
                {
                    mtx.lock();
                    defer mtx.unlock();

                    if (done) break;

                    signal.wait(&mtx);
                }

                while (!inbox.isEmpty()) {
                    if (inbox.get()) |items| {
                        thread_pool.spawn(
                            write,
                            .{ &wait_group, db, items.data },
                        ) catch |err| {
                            debug.print(
                                "threadpool spawn error {s}\n",
                                .{@errorName(err)},
                            );
                            return;
                        };
                        count += 1;
                    }
                }
            }

            debug.print("spawned {d} workers\n", .{count});

            thread_pool.waitAndWork(&wait_group);

            db.flush() catch |err| {
                debug.print(
                    "error flushing db {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
        }
    };

    pub fn run(input: []const u8, opts: lsm.Opts) !void {
        var arena = heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const arena_allocator = arena.allocator();

        lsm.BeginProfile(arena_allocator);
        defer lsm.EndProfile();

        var mailbox = MessageQueue.init();

        const publisher = try std.Thread.spawn(.{}, Reader.publish, .{ input, &mailbox });
        const consumer = try std.Thread.spawn(.{}, Writer.consume, .{ opts, &mailbox });

        consumer.join();
        publisher.join();
    }
};

const MultiProcessImpl = struct {
    const Self = @This();

    pub fn init() !Runnable {
        return .{
            .run = Self.run,
        };
    }

    pub fn run(input: []const u8, opts: lsm.Opts) !void {
        var arena = heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const alloc = arena.allocator();

        var mailbox = try lsm.ProcessMessageQueue.init(alloc, ".");
        defer mailbox.deinit();

        const reader = try std.Thread.spawn(.{}, struct {
            pub fn consume(options: lsm.Opts, inbox: *const lsm.ProcessMessageQueue.ReadIter) void {
                const db = lsm.databaseFromOpts(allocator, options) catch |err| {
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
        }.consume, .{ opts, &mailbox.subscribe() });

        const writer = mailbox.publisher();

        var kvs = std.ArrayList([]const u8).init(allocator);
        defer kvs.deinit();

        var idx: usize = 0;
        var data_row = [2][]const u8{ undefined, undefined };

        const handle = lsm.CsvOpen2(input.ptr, ';', '"', '\\');
        defer lsm.CsvClose(handle);

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
