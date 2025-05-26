const std = @import("std");

/// Creates a generic iterator for type T.
///
/// This function returns a struct that implements the Iterator pattern.
/// It provides a standardized way to iterate over collections of type T.
///
/// Parameters:
///   - T: The type of elements that the iterator will yield
///
/// Returns:
///   A struct type with methods:
///   - init: Creates a new iterator with the given context and functions
///   - deinit: Cleans up resources when iteration is complete
///   - next: Returns the next element or null when iteration is complete
pub fn Iterator(comptime T: type) type {
    return struct {
        const Self = @This();

        deinitFn: ?*const fn (ctx: *anyopaque) void,
        nxtFn: *const fn (ctx: *anyopaque) ?T,
        ptr: *anyopaque,

        pub fn init(
            ptr: *anyopaque,
            nxt: *const fn (ctx: *anyopaque) ?T,
            dnt: ?*const fn (ctx: *anyopaque) void,
        ) Self {
            return .{
                .deinitFn = dnt,
                .nxtFn = nxt,
                .ptr = ptr,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.deinitFn) |func| {
                func(self.ptr);
            }
            self.* = undefined;
        }

        pub fn next(self: *Self) ?T {
            return self.nxtFn(self.ptr);
        }
    };
}

/// Creates a merge iterator that combines multiple iterators into a single sorted stream.
/// This implementation uses a priority queue to efficiently merge multiple iterators.
///
/// Parameters:
///   - T: The type of elements in the iterators
///
/// Returns:
///   A struct that merges multiple iterators into a single sorted stream
pub fn MergeIterator(
    comptime T: type,
    compareFn: *const fn (T, T) std.math.Order,
) type {
    return struct {
        const Self = @This();

        const Item = struct {
            value: T,
            source_index: usize,
        };

        fn itemCompare(context: void, a: Item, b: Item) std.math.Order {
            _ = context;
            return compareFn(a.value, b.value);
        }

        const PriorityQueue = std.PriorityQueue(Item, void, itemCompare);

        allocator: std.mem.Allocator,
        iterators: std.ArrayList(Iterator(T)),
        queue: PriorityQueue,
        compareFn: *const fn (T, T) std.math.Order,

        pub fn init(allocator: std.mem.Allocator) !Self {
            return Self{
                .allocator = allocator,
                .iterators = std.ArrayList(Iterator(T)).init(allocator),
                .queue = PriorityQueue.init(allocator, {}),
                .compareFn = compareFn,
            };
        }

        pub fn deinit(ctx: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx));

            for (self.iterators.items) |*iter| {
                iter.deinit();
            }
            self.iterators.deinit();

            self.queue.deinit();

            self.* = undefined;
        }

        pub fn add(self: *Self, iter: Iterator(T)) !void {
            try self.iterators.append(iter);

            if (self.iterators.items[self.iterators.items.len - 1].next()) |value| {
                try self.queue.add(Item{
                    .value = value,
                    .source_index = self.iterators.items.len - 1,
                });
            }
        }

        pub fn next(ctx: *anyopaque) ?T {
            const self: *Self = @ptrCast(@alignCast(ctx));

            if (self.queue.removeOrNull()) |item| {
                const value = item.value;
                const source_index = item.source_index;

                // Get the next item from the source iterator
                var iter = &self.iterators.items[source_index];
                if (iter.next()) |next_value| {
                    self.queue.add(Item{
                        .value = next_value,
                        .source_index = source_index,
                    }) catch unreachable;
                }

                return value;
            }

            return null;
        }

        pub fn iterator(self: *Self) Iterator(T) {
            return Iterator(T).init(self, Self.next, Self.deinit);
        }
    };
}

/// Creates a filter iterator that wraps an existing Iterator.
/// This allows filtering elements from an iterator based on a predicate function.
///
/// Parameters:
///   - T: The type of elements in the iterator
///
/// Returns:
///   A function that takes a predicate and source iterator and returns a new filtered iterator
pub fn FilterIterator(comptime T: type) type {
    return struct {
        const Self = @This();

        source: Iterator(T),
        predicate: *const fn (item: T) bool,

        pub fn init(source: Iterator(T), predicate: *const fn (item: T) bool) Self {
            return .{
                .source = source,
                .predicate = predicate,
            };
        }

        pub fn iterator(self: *Self) Iterator(T) {
            return Iterator(T).init(self, nextFn, deinitFn);
        }

        fn nextFn(ctx: *anyopaque) ?T {
            const self: *Self = @ptrCast(@alignCast(ctx));
            while (self.source.next()) |item| {
                if (self.predicate(item)) {
                    return item;
                }
            }
            return null;
        }

        fn deinitFn(ctx: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.source.deinit();
        }
    };
}

/// Creates a ScanIterator that first seeks
/// to a "start" position and returns items
/// with a key greater than start and less than
/// or equal to an "end" position.
///
/// Parameters:
///   - T: The type of elements in the iterator
///   - compareFn: Function to compare two elements of type T
///
/// Returns:
///   A struct that implements a scan iterator with start and end bounds
pub fn ScanIterator(
    comptime T: type,
    compareFn: *const fn (T, T) std.math.Order,
) type {
    return struct {
        const Self = @This();

        source: Iterator(T),
        start: ?T,
        end: ?T,
        started: bool,

        pub fn init(source: Iterator(T), start: ?T, end: ?T) Self {
            return .{
                .source = source,
                .start = start,
                .end = end,
                .started = false,
            };
        }

        pub fn iterator(self: *Self) Iterator(T) {
            return Iterator(T).init(self, nextFn, deinitFn);
        }

        pub fn nextFn(ctx: *anyopaque) ?T {
            const self: *Self = @ptrCast(@alignCast(ctx));

            // If we haven't started scanning yet, seek to the start position
            if (!self.started) {
                self.started = true;

                if (self.start) |start| {
                    // Skip items until we find one greater than start
                    while (self.source.next()) |item| {
                        const order = compareFn(item, start);
                        if (order == .eq or order == .gt) {
                            return item;
                        }
                    }
                    return null; // No items found > start
                }
            }

            // For subsequent calls, get the next item and check against end bound
            if (self.source.next()) |item| {
                // If we have an end bound, check if the item is <= end
                if (self.end) |end| {
                    const order = compareFn(item, end);
                    if (order == .gt) {
                        return null; // Item is > end, we're done
                    }
                }
                return item; // Item is within bounds
            }

            return null; // No more items
        }

        pub fn deinitFn(ctx: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.source.deinit();
        }
    };
}

const U32Stream = struct {
    items: []u32,
    nxt: usize,
    len: usize,

    const Self = @This();

    pub fn init(items: []u32) Self {
        return .{
            .items = items,
            .len = items.len,
            .nxt = 0,
        };
    }

    pub fn iterator(self: *Self) Iterator(u32) {
        return Iterator(u32).init(self, nextFn, null);
    }

    fn nextFn(ctx: *anyopaque) ?u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        if (self.nxt < self.items.len) {
            const nxt = self.items[self.nxt];
            self.nxt += 1;
            return nxt;
        }
        return null;
    }
};

test Iterator {
    const testing = std.testing;
    const alloc = testing.allocator;

    var expected = try alloc.alloc(u32, 3);
    defer alloc.free(expected);

    expected[0] = 2;
    expected[1] = 1;
    expected[2] = 3;

    var actual = std.ArrayList(u32).init(alloc);
    defer actual.deinit();

    var stream = U32Stream.init(expected);

    var iter = stream.iterator();
    defer iter.deinit();

    while (iter.next()) |nxt| {
        try actual.append(nxt);
    }

    try testing.expectEqual(expected.len, actual.items.len);
}

test "FilterIterator" {
    const testing = std.testing;
    const alloc = testing.allocator;

    // Create an array of numbers
    var numbers = try alloc.alloc(u32, 6);
    defer alloc.free(numbers);

    numbers[0] = 1;
    numbers[1] = 2;
    numbers[2] = 3;
    numbers[3] = 4;
    numbers[4] = 5;
    numbers[5] = 6;

    var stream = U32Stream.init(numbers);
    const baseIter = stream.iterator();

    const isEven = struct {
        fn filter(n: u32) bool {
            return n % 2 == 0;
        }
    }.filter;

    var filter = FilterIterator(u32).init(baseIter, isEven);
    var iter = filter.iterator();
    defer iter.deinit();

    var actual = std.ArrayList(u32).init(alloc);
    defer actual.deinit();

    while (iter.next()) |n| {
        try actual.append(n);
    }

    try testing.expectEqual(@as(usize, 3), actual.items.len);
    try testing.expectEqual(@as(u32, 2), actual.items[0]);
    try testing.expectEqual(@as(u32, 4), actual.items[1]);
    try testing.expectEqual(@as(u32, 6), actual.items[2]);
}

test "MergeIterator" {
    const testing = std.testing;
    const alloc = testing.allocator;

    var numbers1 = try alloc.alloc(u32, 3);
    defer alloc.free(numbers1);
    numbers1[0] = 1;
    numbers1[1] = 4;
    numbers1[2] = 7;

    var numbers2 = try alloc.alloc(u32, 4);
    defer alloc.free(numbers2);
    numbers2[0] = 2;
    numbers2[1] = 3;
    numbers2[2] = 5;
    numbers2[3] = 8;

    var numbers3 = try alloc.alloc(u32, 2);
    defer alloc.free(numbers3);
    numbers3[0] = 0;
    numbers3[1] = 6;

    var stream1 = U32Stream.init(numbers1);
    var stream2 = U32Stream.init(numbers2);
    var stream3 = U32Stream.init(numbers3);

    const iter1 = stream1.iterator();
    const iter2 = stream2.iterator();
    const iter3 = stream3.iterator();

    const compareU32 = struct {
        fn compare(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.compare;

    var merger = try MergeIterator(u32, compareU32).init(alloc);
    try merger.add(iter1);
    try merger.add(iter2);
    try merger.add(iter3);

    var mergedIter = merger.iterator();
    defer mergedIter.deinit();

    var actual = std.ArrayList(u32).init(alloc);
    defer actual.deinit();

    while (mergedIter.next()) |n| {
        try actual.append(n);
    }

    try testing.expectEqual(@as(usize, 9), actual.items.len);
    try testing.expectEqual(@as(u32, 0), actual.items[0]);
    try testing.expectEqual(@as(u32, 1), actual.items[1]);
    try testing.expectEqual(@as(u32, 2), actual.items[2]);
    try testing.expectEqual(@as(u32, 3), actual.items[3]);
    try testing.expectEqual(@as(u32, 4), actual.items[4]);
    try testing.expectEqual(@as(u32, 5), actual.items[5]);
    try testing.expectEqual(@as(u32, 6), actual.items[6]);
    try testing.expectEqual(@as(u32, 7), actual.items[7]);
    try testing.expectEqual(@as(u32, 8), actual.items[8]);
}

test "ScanIterator" {
    const testing = std.testing;
    const alloc = testing.allocator;

    // Create an array of ordered numbers
    var numbers = try alloc.alloc(u32, 10);
    defer alloc.free(numbers);

    for (0..10) |i| {
        numbers[i] = @intCast(i);
    }

    var stream = U32Stream.init(numbers);
    const baseIter = stream.iterator();

    const compareU32 = struct {
        fn compare(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.compare;

    // Test with start=3, end=7
    {
        var scan = ScanIterator(u32, compareU32).init(baseIter, 3, 7);
        var iter = scan.iterator();
        defer iter.deinit();

        var actual = std.ArrayList(u32).init(alloc);
        defer actual.deinit();

        while (iter.next()) |n| {
            try actual.append(n);
        }

        // Should include 3,4,5,6,7 (values >= 3 and <= 7)
        try testing.expectEqual(@as(usize, 5), actual.items.len);
        try testing.expectEqual(@as(u32, 3), actual.items[0]);
        try testing.expectEqual(@as(u32, 4), actual.items[1]);
        try testing.expectEqual(@as(u32, 5), actual.items[2]);
        try testing.expectEqual(@as(u32, 6), actual.items[3]);
        try testing.expectEqual(@as(u32, 7), actual.items[4]);
    }

    // Reset for another test
    stream = U32Stream.init(numbers);
    const baseIter2 = stream.iterator();

    // Test with no start, end=5
    {
        var scan = ScanIterator(u32, compareU32).init(baseIter2, null, 5);
        var iter = scan.iterator();
        defer iter.deinit();

        var actual = std.ArrayList(u32).init(alloc);
        defer actual.deinit();

        while (iter.next()) |n| {
            try actual.append(n);
        }

        // Should include 0,1,2,3,4,5
        try testing.expectEqual(@as(usize, 6), actual.items.len);
        try testing.expectEqual(@as(u32, 0), actual.items[0]);
        try testing.expectEqual(@as(u32, 1), actual.items[1]);
        try testing.expectEqual(@as(u32, 2), actual.items[2]);
        try testing.expectEqual(@as(u32, 3), actual.items[3]);
        try testing.expectEqual(@as(u32, 4), actual.items[4]);
        try testing.expectEqual(@as(u32, 5), actual.items[5]);
    }

    // Reset for another test
    stream = U32Stream.init(numbers);
    const baseIter3 = stream.iterator();

    // Test with start=7, no end
    {
        var scan = ScanIterator(u32, compareU32).init(baseIter3, 7, null);
        var iter = scan.iterator();
        defer iter.deinit();

        var actual = std.ArrayList(u32).init(alloc);
        defer actual.deinit();

        while (iter.next()) |n| {
            try actual.append(n);
        }

        // Should include 7,8,9
        try testing.expectEqual(@as(usize, 3), actual.items.len);
        try testing.expectEqual(@as(u32, 7), actual.items[0]);
        try testing.expectEqual(@as(u32, 8), actual.items[1]);
        try testing.expectEqual(@as(u32, 9), actual.items[2]);
    }
}
