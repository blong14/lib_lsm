const std = @import("std");
const builtin = @import("builtin");
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
const expect = std.testing.expect;
const Allocator = std.mem.Allocator;

const EOQ = 2;

/// `ProcessMessageQueue` is a System V message queue wrapper.
/// The calling process must have write permission on the message queue
/// in order to send a message, and read permission to receive a message.
pub const ProcessMessageQueue = struct {
    alloc: Allocator,
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
        mdata: []const u8,
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
            .msqid = msqid,
            .msqproj = msqproj,
            .msqtype = 1,
        };
        return msgq;
    }

    /// Remove the message queue
    pub fn deinit(self: *Self) void {
        if (sys.msgctl(self.msqid, sys.IPC_RMID, null) == -1) {
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
        pub fn next(self: ReadIter) ?[]const u8 {
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
        pub fn publish(self: Writer, v: []const u8) MessageQueueError!void {
            var mesg = Message{ .mtype = self.msqtype, .mdata = v };
            switch (errno(sys.msgsnd(self.msqid, &mesg, v.len, 0))) {
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
    var mailbox = try ProcessMessageQueue.init(alloc, ".");
    defer alloc.destroy(mailbox);
    defer mailbox.deinit();

    const reader = mailbox.subscribe();
    const writer = try std.Thread.spawn(.{}, struct {
        pub fn publish(outbox: ProcessMessageQueue.Writer, data: Elem) !void {
            try outbox.publish(std.mem.toBytes(data));
        }
    }.publish, .{ mailbox.publisher(), elem });

    const actual = reader.next();
    const byts = actual.?;
    const value = std.mem.bytesToValue(Elem, byts);
    try testing.expect(value.data == elem.data);
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
    var mailbox = try ProcessMessageQueue.init(alloc, ".");
    defer alloc.destroy(mailbox);
    defer mailbox.deinit();

    // Count all messages
    const reader = mailbox.subscribe();
    const writer = try std.Thread.spawn(.{}, struct {
        pub fn publish(outbox: ProcessMessageQueue.Writer, data: []Elem) !void {
            defer outbox.done() catch |err| {
                std.debug.print("Oops {s}\n", .{@errorName(err)});
            };
            for (data) |elem| {
                try outbox.publish(std.mem.toBytes(elem));
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

/// `Queue` is a many producer, many consumer, non-allocating, thread-safe.
/// Uses a mutex to protect access.
/// The queue does not manage ownership and the user is responsible to
/// manage the storage of the nodes.
pub fn Queue(comptime T: type) type {
    return struct {
        head: ?*Node,
        tail: ?*Node,
        mutex: std.Thread.Mutex,

        pub const Self = @This();

        pub const Node = std.DoublyLinkedList(T).Node;

        /// Initializes a new queue. The queue does not provide a `deinit()`
        /// function, so the user must take care of cleaning up the queue elements.
        pub fn init() Self {
            return Self{
                .head = null,
                .tail = null,
                .mutex = std.Thread.Mutex{},
            };
        }

        /// Appends `node` to the queue.
        /// The lifetime of `node` must be longer than the lifetime of the queue.
        pub fn put(self: *Self, node: *Node) void {
            node.next = null;
            self.mutex.lock();
            defer self.mutex.unlock();
            node.prev = self.tail;
            self.tail = node;
            if (node.prev) |prev_tail| {
                prev_tail.next = node;
            } else {
                assert(self.head == null);
                self.head = node;
            }
        }

        /// Gets a previously inserted node or returns `null` if there is none.
        /// It is safe to `get()` a node from the queue while another thread tries
        /// to `remove()` the same node at the same time.
        pub fn get(self: *Self) ?*Node {
            self.mutex.lock();
            defer self.mutex.unlock();
            const head = self.head orelse return null;
            self.head = head.next;
            if (head.next) |new_head| {
                new_head.prev = null;
            } else {
                self.tail = null;
            }
            // This way, a get() and a remove() are thread-safe with each other.
            head.prev = null;
            head.next = null;
            return head;
        }

        /// Prepends `node` to the front of the queue.
        /// The lifetime of `node` must be longer than the lifetime of the queue.
        pub fn unget(self: *Self, node: *Node) void {
            node.prev = null;
            self.mutex.lock();
            defer self.mutex.unlock();
            const opt_head = self.head;
            self.head = node;
            if (opt_head) |old_head| {
                node.next = old_head;
            } else {
                assert(self.tail == null);
                self.tail = node;
            }
        }

        /// Removes a node from the queue, returns whether node was actually removed.
        /// It is safe to `remove()` a node from the queue while another thread tries
        /// to `get()` the same node at the same time.
        pub fn remove(self: *Self, node: *Node) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (node.prev == null and node.next == null and self.head != node) {
                return false;
            }
            if (node.prev) |prev| {
                prev.next = node.next;
            } else {
                self.head = node.next;
            }
            if (node.next) |next| {
                next.prev = node.prev;
            } else {
                self.tail = node.prev;
            }
            node.prev = null;
            node.next = null;
            return true;
        }

        /// Returns `true` if the queue is currently empty.
        /// Note that in a multi-consumer environment a return value of `false`
        /// does not mean that `get` will yield a non-`null` value!
        pub fn isEmpty(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.head == null;
        }

        /// Dumps the contents of the queue to `stderr`.
        pub fn dump(self: *Self) void {
            self.dumpToStream(std.io.getStdErr().writer()) catch return;
        }

        /// Dumps the contents of the queue to `stream`.
        /// Up to 4 elements from the head are dumped and the tail of the queue is
        /// dumped as well.
        pub fn dumpToStream(self: *Self, stream: anytype) !void {
            const S = struct {
                fn dumpRecursive(
                    s: anytype,
                    optional_node: ?*Node,
                    indent: usize,
                    comptime depth: comptime_int,
                ) !void {
                    try s.writeByteNTimes(' ', indent);
                    if (optional_node) |node| {
                        try s.print("0x{x}={any}\n", .{ @intFromPtr(node), node.data });
                        if (depth == 0) {
                            try s.print("(max depth)\n", .{});
                            return;
                        }
                        try dumpRecursive(s, node.next, indent + 1, depth - 1);
                    } else {
                        try s.print("(null)\n", .{});
                    }
                }
            };
            self.mutex.lock();
            defer self.mutex.unlock();
            try stream.print("head: ", .{});
            try S.dumpRecursive(stream, self.head, 0, 4);
            try stream.print("tail: ", .{});
            try S.dumpRecursive(stream, self.tail, 0, 4);
        }
    };
}

const Context = struct {
    allocator: std.mem.Allocator,
    queue: *Queue(i32),
    put_sum: isize,
    get_sum: isize,
    get_count: usize,
    puts_done: bool,
};

// TODO add lazy evaluated build options and then put puts_per_thread behind
// some option such as: "AggressiveMultithreadedFuzzTest". In the AppVeyor
// CI we would use a less aggressive setting since at 1 core, while we still
// want this test to pass, we need a smaller value since there is so much thrashing
// we would also use a less aggressive setting when running in valgrind
const puts_per_thread = 500;
const put_thread_count = 3;
test "msgqueue.Queue" {
    const plenty_of_memory = try std.heap.page_allocator.alloc(u8, 300 * 1024);
    defer std.heap.page_allocator.free(plenty_of_memory);
    var fixed_buffer_allocator = std.heap.FixedBufferAllocator.init(plenty_of_memory);
    const a = fixed_buffer_allocator.threadSafeAllocator();
    var queue = Queue(i32).init();
    var context = Context{
        .allocator = a,
        .queue = &queue,
        .put_sum = 0,
        .get_sum = 0,
        .puts_done = false,
        .get_count = 0,
    };
    if (builtin.single_threaded) {
        try expect(context.queue.isEmpty());
        {
            var i: usize = 0;
            while (i < put_thread_count) : (i += 1) {
                try expect(startPuts(&context) == 0);
            }
        }
        try expect(!context.queue.isEmpty());
        context.puts_done = true;
        {
            var i: usize = 0;
            while (i < put_thread_count) : (i += 1) {
                try expect(startGets(&context) == 0);
            }
        }
        try expect(context.queue.isEmpty());
    } else {
        try expect(context.queue.isEmpty());
        var putters: [put_thread_count]std.Thread = undefined;
        for (&putters) |*t| {
            t.* = try std.Thread.spawn(.{}, startPuts, .{&context});
        }
        var getters: [put_thread_count]std.Thread = undefined;
        for (&getters) |*t| {
            t.* = try std.Thread.spawn(.{}, startGets, .{&context});
        }
        for (putters) |t|
            t.join();
        @atomicStore(bool, &context.puts_done, true, .SeqCst);
        for (getters) |t|
            t.join();
        try expect(context.queue.isEmpty());
    }
    if (context.put_sum != context.get_sum) {
        std.debug.panic("failure\nput_sum:{} != get_sum:{}", .{ context.put_sum, context.get_sum });
    }
    if (context.get_count != puts_per_thread * put_thread_count) {
        std.debug.panic("failure\nget_count:{} != puts_per_thread:{} * put_thread_count:{}", .{
            context.get_count,
            @as(u32, puts_per_thread),
            @as(u32, put_thread_count),
        });
    }
}

fn startPuts(ctx: *Context) u8 {
    var put_count: usize = puts_per_thread;
    var prng = std.rand.DefaultPrng.init(0xdeadbeef);
    const random = prng.random();
    while (put_count != 0) : (put_count -= 1) {
        std.time.sleep(1); // let the os scheduler be our fuzz
        const x = @as(i32, @bitCast(random.int(u32)));
        const node = ctx.allocator.create(Queue(i32).Node) catch unreachable;
        node.* = .{
            .prev = undefined,
            .next = undefined,
            .data = x,
        };
        ctx.queue.put(node);
        _ = @atomicRmw(isize, &ctx.put_sum, .Add, x, .SeqCst);
    }
    return 0;
}

fn startGets(ctx: *Context) u8 {
    while (true) {
        const last = @atomicLoad(bool, &ctx.puts_done, .SeqCst);
        while (ctx.queue.get()) |node| {
            std.time.sleep(1); // let the os scheduler be our fuzz
            _ = @atomicRmw(isize, &ctx.get_sum, .Add, node.data, .SeqCst);
            _ = @atomicRmw(usize, &ctx.get_count, .Add, 1, .SeqCst);
        }
        if (last) return 0;
    }
}

test "msgqueue.Queue single-threaded" {
    var queue = Queue(i32).init();
    try expect(queue.isEmpty());
    var node_0 = Queue(i32).Node{
        .data = 0,
        .next = undefined,
        .prev = undefined,
    };
    queue.put(&node_0);
    try expect(!queue.isEmpty());
    var node_1 = Queue(i32).Node{
        .data = 1,
        .next = undefined,
        .prev = undefined,
    };
    queue.put(&node_1);
    try expect(!queue.isEmpty());
    try expect(queue.get().?.data == 0);
    try expect(!queue.isEmpty());
    var node_2 = Queue(i32).Node{
        .data = 2,
        .next = undefined,
        .prev = undefined,
    };
    queue.put(&node_2);
    try expect(!queue.isEmpty());
    var node_3 = Queue(i32).Node{
        .data = 3,
        .next = undefined,
        .prev = undefined,
    };
    queue.put(&node_3);
    try expect(!queue.isEmpty());
    try expect(queue.get().?.data == 1);
    try expect(!queue.isEmpty());
    try expect(queue.get().?.data == 2);
    try expect(!queue.isEmpty());
    var node_4 = Queue(i32).Node{
        .data = 4,
        .next = undefined,
        .prev = undefined,
    };
    queue.put(&node_4);
    try expect(!queue.isEmpty());
    try expect(queue.get().?.data == 3);
    node_3.next = null;
    try expect(!queue.isEmpty());
    queue.unget(&node_3);
    try expect(queue.get().?.data == 3);
    try expect(!queue.isEmpty());
    try expect(queue.get().?.data == 4);
    try expect(queue.isEmpty());
    try expect(queue.get() == null);
    try expect(queue.isEmpty());
    // unget an empty queue
    queue.unget(&node_4);
    try expect(queue.tail == &node_4);
    try expect(queue.head == &node_4);
    try expect(queue.get().?.data == 4);
    try expect(queue.get() == null);
    try expect(queue.isEmpty());
}

test "msgqueue.Queue dump" {
    const mem = std.mem;
    var buffer: [1024]u8 = undefined;
    var expected_buffer: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buffer);
    var queue = Queue(i32).init();
    // Test empty stream
    fbs.reset();
    try queue.dumpToStream(fbs.writer());
    try expect(mem.eql(u8, buffer[0..fbs.pos],
        \\head: (null)
        \\tail: (null)
        \\
    ));
    // Test a stream with one element
    var node_0 = Queue(i32).Node{
        .data = 1,
        .next = undefined,
        .prev = undefined,
    };
    queue.put(&node_0);
    fbs.reset();
    try queue.dumpToStream(fbs.writer());
    var expected = try std.fmt.bufPrint(expected_buffer[0..],
        \\head: 0x{x}=1
        \\ (null)
        \\tail: 0x{x}=1
        \\ (null)
        \\
    , .{ @intFromPtr(queue.head), @intFromPtr(queue.tail) });
    try expect(mem.eql(u8, buffer[0..fbs.pos], expected));
    // Test a stream with two elements
    var node_1 = Queue(i32).Node{
        .data = 2,
        .next = undefined,
        .prev = undefined,
    };
    queue.put(&node_1);
    fbs.reset();
    try queue.dumpToStream(fbs.writer());
    expected = try std.fmt.bufPrint(expected_buffer[0..],
        \\head: 0x{x}=1
        \\ 0x{x}=2
        \\  (null)
        \\tail: 0x{x}=2
        \\ (null)
        \\
    , .{ @intFromPtr(queue.head), @intFromPtr(queue.head.?.next), @intFromPtr(queue.tail) });
    try expect(mem.eql(u8, buffer[0..fbs.pos], expected));
}
