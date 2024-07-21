const std = @import("std");

const kv = @import("kv.zig");

const Allocator = std.mem.Allocator;
const Order = std.math.Order;
const KV = kv.KV;

pub fn TableMap(comptime K: type, comptime V: type, comptime compareFn: fn (a: K, b: V) Order) type {
    return struct {
        const Self = @This();

        const TableMapError = error{
            Empty,
            NotFound,
            OutOfMemory,
        };

        const MapEntryTable = std.ArrayList(V);

        cap: usize,
        impl: MapEntryTable,

        pub fn init(alloc: Allocator, capacity: usize) !Self {
            var impl = MapEntryTable.init(alloc);
            try impl.ensureTotalCapacity(capacity);
            return .{
                .cap = capacity,
                .impl = impl,
            };
        }

        pub fn deinit(self: *Self) void {
            self.impl.deinit();
        }

        pub fn findIndex(self: Self, key: K, low: usize, high: usize) usize {
            var start = low;
            var end = high;
            while (start < end) {
                const mid: usize = start + (end - start) / 2;
                const current_item = self.impl.items[mid];
                switch (compareFn(key, current_item)) {
                    .lt => end = mid,
                    .gt => start = mid + 1,
                    .eq => return mid,
                }
            }
            return start;
        }

        fn equalto(self: Self, key: K, idx: usize) bool {
            const entry = self.impl.items[idx];
            return compareFn(key, entry) == .eq;
        }

        fn greaterthan(self: Self, key: K, idx: usize) bool {
            const entry = self.impl.items[idx];
            return compareFn(key, entry) == .gt;
        }

        pub fn count(self: Self) usize {
            return self.impl.items.len;
        }

        pub fn getEntryByIdx(self: Self, idx: usize) TableMapError!V {
            const cnt = self.count();
            if (idx >= cnt) {
                return TableMapError.NotFound;
            }
            return self.impl.items[idx];
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
            return self.impl.items[idx];
        }

        pub fn put(self: *Self, key: K, value: V) TableMapError!void {
            const cnt: usize = self.count();
            if (cnt == self.cap) {
                return TableMapError.OutOfMemory;
            }
            if ((cnt == 0) or (self.greaterthan(key, cnt - 1))) {
                return try self.append(value);
            }
            const idx = self.findIndex(key, 0, cnt - 1);
            try self.impl.insert(idx, value);
        }

        pub fn append(self: *Self, value: V) !void {
            if (self.count() == self.cap) {
                return TableMapError.OutOfMemory;
            }
            try self.impl.append(value);
        }
    };
}

test TableMap {
    const testing = std.testing;

    var tm = try TableMap([]const u8, KV, KV.order).init(testing.allocator, std.mem.page_size);
    defer tm.deinit();

    const key = "__key__";
    const item = KV{
        .hash = 0,
        .key = key,
        .value = "__value__",
    };

    try tm.put(key, item);

    const actual = try tm.get(key);

    try testing.expectEqualStrings(actual.value, item.value);
}

fn order(a: u16, b: u16) Order {
    return std.math.order(a, b);
}

test "TableMap getEntryByIdx" {
    const testing = std.testing;

    const cap = 4;

    var tm = try TableMap(u16, u16, order).init(testing.allocator, cap);
    defer tm.deinit();

    const keys = [4]u16{4,3,2,1};
    for (keys) |key| {
        try tm.put(key, key);
    }

    var actual = [4]u16{0,0,0,0};
    for (keys, 0..) |_, i| {
        actual[i] = try tm.getEntryByIdx(i);
    }

    try testing.expectEqualSlices(u16, &[4]u16{1,2,3,4}, &actual);
}
