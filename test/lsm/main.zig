const std = @import("std");

const clap = @import("clap");
const lsm = @import("lsm");
const msgpack = @import("msgpack");

const debug = std.debug;
const fs = std.fs;
const heap = std.heap;
const io = std.io;
const mem = std.mem;

const Allocator = mem.Allocator;

const KV = lsm.KV;
const FixedBufferStream = std.io.FixedBufferStream([]u8);

const pack = msgpack.Pack(
    *FixedBufferStream, // writer
    *FixedBufferStream, // reader
    FixedBufferStream.WriteError,
    FixedBufferStream.ReadError,
    FixedBufferStream.write,
    FixedBufferStream.read,
);

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
    const input = res.args.input orelse "trips.txt";
    const sst_capacity = res.args.sst_capacity orelse default_opts.sst_capacity;
    const wal_capacity = default_opts.wal_capacity;
    const mode = res.args.mode orelse Mode.singlethreaded;
    const alloc = gpa.allocator();

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

    const impl = if (mode == .singlethreaded)
        try SingleThreadedImpl.init(alloc, db)
    else if (mode == .multithreaded)
        try MultiThreadedImpl.init(alloc, db)
    else if (mode == .multiprocess)
        try MultiProcessImpl.init(alloc, db)
    else
        @panic("Unsupported mode");

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

    pub fn init(alloc: Allocator, db: *lsm.Database) !Runnable {
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
        this.alloc.destroy(self);
    }

    pub fn run(input: []const u8) anyerror!void {
        defer self.deinit();

        lsm.BeginProfile(self.alloc);
        defer lsm.EndProfile();

        const rows = try parse(self.alloc, input);
        defer rows.deinit();

        write(rows);

        read();
    }

    fn parse(alloc: Allocator, input: []const u8) !std.ArrayList([2][]const u8) {
        var timer = lsm.BlockProfiler.start("parse");
        defer timer.end();

        const file = fs.cwd().openFile(input, .{}) catch |err| {
            debug.print("open file error {s}\n", .{@errorName(err)});
            return err;
        };
        defer file.close();

        const stat = try file.stat();
        timer.withBytes(stat.size);

        var buffer = [_]u8{0} ** mem.page_size;
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
                    out.appendAssumeCapacity(row);
                    count += 1;
                    row = [2][]const u8{ undefined, undefined };
                    idx = 0;
                },
            }
        }
        return out;
    }

    fn write(input: std.ArrayList([2][]const u8)) void {
        var timer = lsm.BlockProfiler.start("write");
        defer timer.end();

        var bytes: u64 = 0;
        for (input.items) |row| {
            self.db.write(row[0], row[1]) catch |err| {
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

    fn read() void {
        var timer = lsm.BlockProfiler.start("read");
        defer timer.end();

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
            //debug.print(
            //    "KV::\t{s}\t{s}\n",
            //    .{ iter.value().key, iter.value().value },
            //);
            count += 1;
        }
    }
};

const BufferedEncoder = struct {
    cap: usize,
    data: std.ArrayList(msgpack.Payload),
    stream: lsm.MessageQueue([]u8).Writer,

    const Self = @This();

    pub fn init(alloc: Allocator, stream: lsm.MessageQueue([]u8).Writer, capacity: usize) Self {
        return .{
            .cap = capacity,
            .data = std.ArrayList(msgpack.Payload).init(alloc),
            .stream = stream,
        };
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.* = undefined;
    }

    pub fn write(self: *Self, alloc: Allocator, payload: msgpack.Payload) !void {
        if (self.data.items.len >= self.cap) {
            try self.flush(alloc);
            self.data.clearRetainingCapacity();
        } else {
            try self.data.append(payload);
        }
    }

    pub fn flush(self: *Self, alloc: Allocator) !void {
        var arena = heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        const allocator = arena.allocator();

        var kvs = try msgpack.Payload.arrPayload(self.data.items.len, allocator);
        defer kvs.free(allocator);

        for (self.data.items, 0..) |kv, i| {
            try kvs.setArrElement(i, kv);
        }

        var arr: [0xffff_f]u8 = mem.zeroes([0xffff_f]u8);
        var buf = io.fixedBufferStream(&arr);

        var encoder = pack.init(&buf, &buf);
        try encoder.write(kvs);

        self.stream.publish(buf.buffer) catch |err| {
            debug.print(
                "error publishing row {s}\n",
                .{@errorName(err)},
            );
            return;
        };
    }
};

const MultiThreadedImpl = struct {
    alloc: Allocator,
    db: *lsm.Database,

    const Self = @This();

    var self: *Self = undefined;

    pub fn init(alloc: Allocator, db: *lsm.Database) !Runnable {
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
        this.alloc.destroy(self);
        this.* = undefined;
    }

    const Reader = struct {
        pub fn publish(input: []const u8, outbox: lsm.MessageQueue([]u8).Writer) void {
            const file = std.fs.cwd().openFile(input, .{}) catch |err| {
                std.debug.print(
                    "not able to open file {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
            defer file.close();

            const fileReader = file.reader();
            var buffer = [_]u8{0} ** mem.page_size;
            var csv_file = lsm.CSV.init(fileReader, &buffer, .{}) catch |err| {
                std.debug.print("publish error {s}\n", .{@errorName(err)});
                return;
            };

            var buffered_encoder = BufferedEncoder.init(
                self.alloc,
                outbox,
                mem.page_size,
            );
            defer buffered_encoder.deinit();

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

                        const payload = item.toPayload(self.alloc) catch return;
                        buffered_encoder.write(self.alloc, payload) catch return;

                        count += 1;
                        row = [2][]const u8{ undefined, undefined };
                        idx = 0;
                    },
                }
            }

            buffered_encoder.flush(self.alloc) catch return;

            outbox.done() catch return;
        }
    };

    const Writer = struct {
        pub fn consume(db: *lsm.Database, inbox: lsm.MessageQueue([]u8).ReadIter) void {
            const alloc = heap.c_allocator;

            var count: usize = 0;
            while (inbox.next()) |batch| {
                var arr = [_]u8{0}; // noop
                var write_buffer = std.io.fixedBufferStream(&arr);
                var read_buffer = std.io.fixedBufferStream(batch);

                var decoder = pack.init(&write_buffer, &read_buffer);
                const buf = decoder.read(alloc) catch return;
                const len = buf.getArrLen() catch return;

                for (0..len) |i| {
                    const item = buf.getArrElement(i) catch return;
                    var iter = item.map.keyIterator();
                    if (iter.next()) |key| {
                        const value = item.map.get(key.*).?;
                        db.write(key.*, value.str.value()) catch |err| {
                            std.debug.print(
                                "db write error count {d} key {s} {s}\n",
                                .{ count, key.*, @errorName(err) },
                            );
                            return;
                        };
                        count += 1;
                    }
                }

                buf.free(alloc);
            }
            db.flush() catch |err| {
                std.debug.print(
                    "error flushing db {s}\n",
                    .{@errorName(err)},
                );
                return;
            };
        }
    };

    pub fn run(input: []const u8) !void {
        defer self.deinit();

        lsm.BeginProfile(self.alloc);
        defer lsm.EndProfile();

        var mailbox = try lsm.MessageQueue([]u8).init(self.alloc, ".");
        defer self.alloc.destroy(mailbox);
        defer mailbox.deinit();

        const consumer = try std.Thread.spawn(.{}, Writer.consume, .{ self.db, mailbox.subscribe() });
        const publisher = try std.Thread.spawn(.{}, Reader.publish, .{ input, mailbox.publisher() });

        consumer.join();
        publisher.join();
    }
};

const MultiProcessImpl = struct {
    alloc: Allocator,
    db: *lsm.Database,

    const Self = @This();

    var self: *Self = undefined;

    pub fn init(alloc: Allocator, db: *lsm.Database) !Runnable {
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
        this.alloc.destroy(self);
        this.* = undefined;
    }

    pub fn run(_: []const u8) !void {
        defer self.deinit();
        debug.print("not implemented\n", .{});
    }
};
