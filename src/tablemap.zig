const std = @import("std");

const Allocator = std.mem.Allocator;
const Order = std.math.Order;

pub fn TableMap(
    comptime K: type,
    comptime V: type,
    comptime compareFn: fn (a: K, b: K) Order,
) type {
    return struct {
        const Self = @This();

        const TableMapError = error{
            Empty,
            NotFound,
            OutOfMemory,
        };

        const Entry = struct {
            key: K,
            value: V,
        };

        cap: usize,
        ximpl: std.MultiArrayList(Entry),

        pub fn init(alloc: Allocator, capacity: usize) !Self {
            var ximpl = std.MultiArrayList(Entry){};
            try ximpl.ensureTotalCapacity(alloc, capacity);
            return .{
                .cap = capacity,
                .ximpl = ximpl,
            };
        }

        pub fn deinit(self: *Self, alloc: Allocator) void {
            self.ximpl.deinit(alloc);
        }

        pub fn findIndex(self: Self, key: K, low: usize, high: usize) usize {
            var start = low;
            var end = high;
            while (start < end) {
                const mid: usize = start + (end - start) / 2;
                const current_item = self.ximpl.items(.key)[mid];
                switch (compareFn(key, current_item)) {
                    .lt => end = mid,
                    .gt => start = mid + 1,
                    .eq => return mid,
                }
            }
            return start;
        }

        inline fn equalto(self: Self, key: K, idx: usize) bool {
            const entry = self.ximpl.get(idx);
            return compareFn(key, entry.key) == .eq;
        }

        inline fn greaterthan(self: Self, key: K, idx: usize) bool {
            const entry = self.ximpl.get(idx);
            return compareFn(key, entry.key) == .gt;
        }

        pub fn count(self: Self) usize {
            return self.ximpl.items(.key).len;
        }

        pub fn getEntryByIdx(self: Self, idx: usize) TableMapError!V {
            const cnt = self.count();
            if (idx >= cnt) {
                return TableMapError.NotFound;
            }

            return self.ximpl.get(idx).value;
        }

        pub fn get(self: Self, key: K) TableMapError!V {
            const cnt = self.count();
            if (cnt == 0) {
                return TableMapError.NotFound;
            }

            const idx = self.findIndex(key, 0, cnt - 1);
            if ((idx == cnt) or !self.equalto(key, idx)) {
                return TableMapError.NotFound;
            }

            return self.ximpl.get(idx).value;
        }

        pub fn put(self: *Self, key: K, value: V) TableMapError!void {
            const cnt: usize = self.count();
            if (cnt == self.cap) {
                return TableMapError.OutOfMemory;
            } else if ((cnt == 0) or (self.greaterthan(key, cnt - 1))) {
                self.ximpl.appendAssumeCapacity(Entry{ .key = key, .value = value });
                return;
            }

            const idx = self.findIndex(key, 0, cnt - 1);
            if (self.equalto(key, idx)) {
                self.ximpl.set(idx, Entry{ .key = key, .value = value });
            } else {
                self.ximpl.insertAssumeCapacity(idx, Entry{ .key = key, .value = value });
            }

            return;
        }
    };
}

fn order(a: u16, b: u16) Order {
    return std.math.order(a, b);
}

test TableMap {
    const testing = std.testing;
    const alloc = testing.allocator;

    var tm = try TableMap(u16, u16, order).init(alloc, std.mem.page_size);
    defer tm.deinit(alloc);

    const key = 4;

    {
        try tm.put(key, key);

        const actual = try tm.get(key);

        try testing.expectEqual(actual, key);
    }

    {
        const expected = 5;

        try tm.put(key, expected);

        const actual = try tm.get(key);

        try testing.expectEqual(tm.count(), 1);
        try testing.expectEqual(actual, expected);
    }
}

test "TableMap getEntryByIdx" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const cap = 4;

    var tm = try TableMap(u16, u16, order).init(alloc, cap);
    defer tm.deinit(alloc);

    const keys = [4]u16{ 4, 3, 2, 1 };
    for (keys) |key| {
        try tm.put(key, key);
    }

    var actual = [4]u16{ 0, 0, 0, 0 };
    for (keys, 0..) |_, i| {
        actual[i] = try tm.getEntryByIdx(i);
    }

    try testing.expectEqualSlices(u16, &[4]u16{ 1, 2, 3, 4 }, &actual);
}
