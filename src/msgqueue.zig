const std = @import("std");
const c = @cImport({
    @cInclude("errno.h");
    @cInclude("stdio.h");
    @cInclude("stdlib.h");
});
const sys = @cImport({
    @cInclude("sys/ipc.h");
    @cInclude("sys/msg.h");
    @cInclude("sys/types.h");
});

const KV = @import("kv.zig").KV;

const assert = std.debug.assert;
const errno = std.posix.errno;
const Allocator = std.mem.Allocator;

const EOQ = 2;

/// `ProcessMessageQueue` is a System V message queue wrapper.
/// The calling process must have write permission on the message queue
/// in order to send a message, and read permission to receive a message.
pub fn ProcessMessageQueue(comptime T: type) type {
    return struct {
        alloc: Allocator,
        msgsize: usize,
        msqid: c_int,
        msqproj: c_int,
        msqtype: c_long,

        const Self = @This();

        const MessageQueueError = error{
            EOQ, // queue has been closed
            OutOfMemory,
            ReadError,
            WriteError,
        };

        const Message = struct {
            mtype: c_long,
            mdata: T,
        };

        /// Creates a `ProcessMessageQueue` with msqid derived from the given file path.
        pub fn init(alloc: Allocator, path: [*c]const u8) MessageQueueError!*Self {
            const msqproj = 1;
            const key = sys.ftok(path, msqproj);
            if (key == -1) {
                c.perror("unable to create msqid");
                return MessageQueueError.ReadError;
            }

            const msqid = sys.msgget(key, 0o666 | sys.IPC_CREAT);
            if (msqid == -1) {
                c.perror("unable to get message queue");
                return MessageQueueError.WriteError;
            }

            const msgq = try alloc.create(Self);
            msgq.* = .{
                .alloc = alloc,
                .msgsize = @sizeOf(T),
                .msqid = msqid,
                .msqproj = msqproj,
                .msqtype = 1,
            };
            return msgq;
        }

        /// Remove the message queue
        pub fn deinit(self: *Self) void {
            if (sys.msgctl(self.*.msqid, sys.IPC_RMID, null) == -1) {
                c.perror("unable to destroy message queue");
            }
            self.* = undefined;
        }

        /// Reader consumes messages off the queue.
        /// The reader is active until there is an error
        /// or the `Done` message is consumed.
        pub const ReadIter = struct {
            msqid: c_int,
            msgsize: usize,

            /// Consumes the next element from the queue and returns it.
            /// returns null when done reading.
            pub fn next(self: ReadIter) ?T {
                var buf: Message = undefined;
                switch (errno(sys.msgrcv(self.msqid, &buf, self.msgsize, 0, 0))) {
                    .SUCCESS => {
                        if (buf.mtype == EOQ) {
                            return null;
                        }
                        return buf.mdata;
                    },
                    else => {
                        c.perror("unable to read message");
                        return null;
                    },
                }
            }
        };

        /// Subscribe to receive messages from this message queue.
        pub fn subscribe(self: Self) ReadIter {
            return .{
                .msqid = self.msqid,
                .msgsize = std.mem.page_size,
            };
        }

        /// Writer publishes messages to the queue.
        /// The writer must call `done` to signal
        /// that the subscriber should stop consuming messages.
        pub const Writer = struct {
            msqid: c_int,
            msgsize: usize,
            msqtype: c_long,

            /// Publishes a new element to the back of the queue.
            pub fn publish(self: Writer, v: T) MessageQueueError!void {
                var mesg = Message{ .mtype = self.msqtype, .mdata = v };
                switch (errno(sys.msgsnd(self.msqid, &mesg, 64, 0))) {
                    .SUCCESS => return,
                    .IDRM => return MessageQueueError.EOQ,
                    else => {
                        c.perror("unable to send message");
                        return MessageQueueError.WriteError;
                    },
                }
            }

            /// Signals consumers that this writer is done sending messages.
            pub fn done(self: Writer) MessageQueueError!void {
                var end: Message = .{ .mtype = EOQ, .mdata = undefined };
                switch (errno(sys.msgsnd(self.msqid, &end, self.msgsize, 0))) {
                    .SUCCESS => return,
                    .IDRM => return MessageQueueError.EOQ,
                    else => {
                        c.perror("unable to send message");
                        return MessageQueueError.WriteError;
                    },
                }
            }
        };

        /// Create a publisher to send messages on the queue.
        pub fn publisher(self: Self) Writer {
            return .{
                .msqid = self.msqid,
                .msgsize = std.mem.page_size,
                .msqtype = self.msqtype,
            };
        }
    };
}

test ProcessMessageQueue {
    const testing = std.testing;
    const Elem = extern struct {
        data: usize,
        const Self = @This();
    };

    const elem: Elem = .{ .data = 1 };

    // Create a mailbox to read and write messages to.
    // The main thread will be in charge of cleaning up the mailbox
    var alloc = testing.allocator;
    var mailbox = try ProcessMessageQueue(Elem).init(alloc, ".");
    defer alloc.destroy(mailbox);
    defer mailbox.deinit();

    const reader = mailbox.subscribe();
    const writer = try std.Thread.spawn(.{}, struct {
        pub fn publish(outbox: ProcessMessageQueue(Elem).Writer, data: Elem) !void {
            try outbox.publish(data);
        }
    }.publish, .{ mailbox.publisher(), elem });

    const actual = reader.next();
    try testing.expect(actual.?.data == elem.data);
    writer.join();
}

test "Test count" {
    const testing = std.testing;
    const Elem = extern struct {
        data: usize,
        const Self = @This();
    };

    var elems = [_]Elem{ .{ .data = 1 }, .{ .data = 2 } };

    // Create a mailbox to read and write messages to.
    // The main thread will be in charge of cleaning up the mailbox
    var alloc = testing.allocator;
    var mailbox = try ProcessMessageQueue(Elem).init(alloc, ".");
    defer alloc.destroy(mailbox);
    defer mailbox.deinit();

    // Count all messages
    const reader = mailbox.subscribe();
    const writer = try std.Thread.spawn(.{}, struct {
        pub fn publish(outbox: ProcessMessageQueue(Elem).Writer, data: []Elem) !void {
            defer outbox.done() catch |err| {
                std.debug.print("Oops {s}\n", .{@errorName(err)});
            };
            for (data) |elem| {
                try outbox.publish(elem);
            }
        }
    }.publish, .{ mailbox.publisher(), &elems });

    var count: u8 = 0;
    while (reader.next()) |_| {
        count += 1;
    }
    try testing.expect(count == elems.len);
    writer.join();
}

pub fn ThreadSafeQueue(comptime T: type) type {
    return struct {
        worker_owned: std.ArrayListUnmanaged(T),
        /// Protected by `mutex`.
        shared: std.ArrayListUnmanaged(T),
        mutex: std.Thread.Mutex,
        state: State,

        const Self = @This();

        pub const State = enum { wait, run };

        pub const empty: Self = .{
            .worker_owned = .empty,
            .shared = .empty,
            .mutex = .{},
            .state = .wait,
        };

        pub fn deinit(self: *Self, gpa: Allocator) void {
            self.worker_owned.deinit(gpa);
            self.shared.deinit(gpa);
            self.* = undefined;
        }

        /// Must be called from the worker thread.
        pub fn check(self: *Self) ?[]T {
            assert(self.worker_owned.items.len == 0);
            {
                self.mutex.lock();
                defer self.mutex.unlock();
                assert(self.state == .run);
                if (self.shared.items.len == 0) {
                    // self.state = .wait;
                    return null;
                }
                std.mem.swap(std.ArrayListUnmanaged(T), &self.worker_owned, &self.shared);
            }
            const result = self.worker_owned.items;
            self.worker_owned.clearRetainingCapacity();
            return result;
        }

        /// Adds items to the queue, returning true if and only if the worker
        /// thread is waiting. Thread-safe.
        /// Not safe to call from the worker thread.
        pub fn enqueue(self: *Self, gpa: Allocator, items: []const T) error{OutOfMemory}!bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            try self.shared.appendSlice(gpa, items[0..]);
            return switch (self.state) {
                .run => false,
                .wait => {
                    self.state = .run;
                    return true;
                },
            };
        }

        /// Safe only to call exactly once when initially starting the worker.
        pub fn start(self: *Self) bool {
            assert(self.state == .wait);
            // if (self.shared.items.len == 0) return false;
            self.state = .run;
            return true;
        }
    };
}

pub fn ThreadMessageQueue(comptime T: type) type {
    return struct {
        alloc: std.heap.ThreadSafeAllocator,
        queue: ThreadSafeQueue(Message),

        const Self = @This();

        pub fn init(allocator: Allocator) !*Self {
            var alloc = std.heap.ThreadSafeAllocator{ .child_allocator = allocator };

            const tmq = try alloc.allocator().create(ThreadMessageQueue(T));
            tmq.* = .{
                .alloc = alloc,
                .queue = ThreadSafeQueue(Message){
                    .mutex = .{},
                    .state = .wait,
                    .shared = try std.ArrayListUnmanaged(Message).initCapacity(alloc.allocator(), std.mem.page_size),
                    .worker_owned = try std.ArrayListUnmanaged(Message).initCapacity(alloc.allocator(), std.mem.page_size),
                },
            };

            _ = tmq.queue.start();

            return tmq;
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit(self.alloc.allocator());
            self.* = undefined;
        }

        /// Reader consumes messages off the queue.
        /// The reader is active until there is an error
        /// or the `Done` message is consumed.
        pub const ReadIter = struct {
            mtx: std.Thread.Mutex,
            nxt: usize,
            inbox: *ThreadSafeQueue(Message),
            outbox: std.ArrayList(Message),

            pub fn deinit(reader: *ReadIter) void {
                reader.outbox.deinit();
            }

            /// Consumes the next element from the queue and returns it.
            /// returns null when done reading.
            pub fn next(reader: *ReadIter) ?T {
                if (reader.inbox.check()) |batch| {
                    reader.mtx.lock();
                    reader.outbox.appendSlice(batch) catch return null;
                    reader.mtx.unlock();
                }

                {
                    reader.mtx.lock();
                    defer reader.mtx.unlock();

                    const items = reader.outbox.items;
                    if (reader.nxt < items.len) {
                        const nxt = items[reader.nxt];
                        if (nxt.mtype == EOQ) {
                            return null;
                        }
                        reader.nxt += 1;
                        return nxt.mdata;
                    }
                }

                return null;
            }
        };

        /// Subscribe to receive messages from this message queue.
        pub fn subscribe(self: *Self) ReadIter {
            return .{
                .mtx = .{},
                .nxt = 0,
                .inbox = &self.queue,
                .outbox = std.ArrayList(Message).init(self.alloc.allocator()),
            };
        }

        const Message = struct {
            mtype: c_long,
            mdata: T,
        };

        /// Writer publishes messages to the queue.
        /// The writer must call `done` to signal
        /// that the subscriber should stop consuming messages.
        pub const Writer = struct {
            allocator: std.heap.ThreadSafeAllocator,
            queue: *ThreadSafeQueue(Message),
            msqtype: c_long,

            /// Publishes a new element to the back of the queue.
            pub fn publish(w: *Writer, v: T) !void {
                const mesg = Message{ .mtype = w.msqtype, .mdata = v };
                _ = try w.queue.enqueue(w.allocator.allocator(), &[1]Message{mesg});
            }

            /// Signals consumers that this writer is done sending messages.
            pub fn done(w: *Writer) !void {
                const end: Message = .{ .mtype = EOQ, .mdata = undefined };
                _ = try w.queue.enqueue(w.allocator.allocator(), &[1]Message{end});
            }
        };

        /// Create a publisher to send messages on the queue.
        pub fn publisher(self: *Self) Writer {
            return .{
                .msqtype = 1,
                .allocator = self.alloc,
                .queue = &self.queue,
            };
        }
    };
}

test ThreadMessageQueue {
    const testing = std.testing;

    // Create a mailbox to read and write messages to.
    // The main thread will be in charge of cleaning up the mailbox
    var alloc = testing.allocator;
    var mailbox = try ThreadMessageQueue([]const u8).init(alloc);
    defer alloc.destroy(mailbox);
    defer mailbox.deinit();

    var reader = mailbox.subscribe();
    defer reader.deinit();

    const writer = try std.Thread.spawn(.{}, struct {
        pub fn publish(outbox: *ThreadMessageQueue([]const u8).Writer) !void {
            const kv = KV.init("__key__", "__value__");

            var buffer: [std.mem.page_size]u8 = undefined;
            const data = try kv.encode(&buffer);

            try outbox.publish(data);
        }
    }.publish, .{@constCast(&mailbox.publisher())});

    writer.join();

    const data = reader.next().?;

    var actual: KV = undefined;
    try actual.decode(data);

    try testing.expectEqualStrings(actual.key, "__key__");
}
