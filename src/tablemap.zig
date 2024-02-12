const std = @import("std");
const Allocator = std.mem.Allocator;

const MapEntry = struct {
    key: u64,
    value: []const u8,
};

const TableMapError = error{
    NotFound,
};
    
const MapEntryTable = std.MultiArrayList(MapEntry);

pub const TableMap = struct {
    gpa: Allocator,
    impl: *MapEntryTable,
    
    const Self = @This();

    pub fn init(alloc: Allocator) !*TableMap {
        var impl = try alloc.create(MapEntryTable);
        impl.* = MapEntryTable{};
        const tm = try alloc.create(TableMap);
        tm.* = TableMap{ .gpa = alloc, .impl = impl };
        return tm;
    }

    pub fn deinit(self: *Self) void {
        self.impl.deinit(self.gpa);
        self.gpa.destroy(self.impl);
        self.* = undefined;
    }

    fn findIndex(self: Self, key: u64, low: usize, high: usize) !usize {
        if (high < low) {
            return high + 1;
        }
        const mid = low + ((high - low) / 2);
        const entry = self.impl.get(mid);
        if (key < entry.key) {
            if (mid == 0) return mid;
            return self.findIndex(key, low, mid - 1);
        } else if (key == entry.key) {
            return mid;
        } else {
            return self.findIndex(key, mid + 1, high);
        }
    }

    fn equalto(self: Self, key: u64, idx: usize) bool {
        const entry = self.impl.get(idx);
        return key == entry.key;
    }

    fn greaterthan(self: Self, key: u64, idx: usize) bool {
        const entry = self.impl.get(idx);
        return key > entry.key;
    }

    pub fn count(self: Self) usize {
        return self.impl.len;
    }

    pub fn get(self: Self, k: u64) TableMapError![]const u8 {
        const cnt = self.count();
        if (cnt == 0) return TableMapError.NotFound;
        const idx = try self.findIndex(k, 0, cnt - 1);
        if ((idx == cnt) or !self.equalto(k, idx)) {
            return TableMapError.NotFound;
        }
        return self.impl.get(idx).value;
    }

    pub fn put(self: *Self, k: u64, v: []const u8) !void {
        const cnt: usize = self.count();
        if ((cnt == 0) or (self.greaterthan(k, cnt-1))) {
            return try self.impl.append(self.gpa, MapEntry{ .key = k, .value = v });
        } 
        const idx = try self.findIndex(k, 0, cnt - 1);
        try self.impl.insert(self.gpa, idx, MapEntry{ .key = k, .value = v });
    }

    pub const Result = struct {
        key: u64,
        value: []const u8,
    };

    pub const Iterator = struct {
        idx: usize,
        k: u64,
        v: []const u8,
        data: *Self,

        pub fn key(it: Iterator) u64 {
            return it.k;
        }

        pub fn value(it: Iterator) []const u8 {
            return it.v;
        }

        pub fn next(it: *Iterator) bool {
            const v = it.data.impl.items(.value)[it.*.idx];
            const k = it.data.impl.items(.key)[it.*.idx];
            it.*.k = k;
            it.*.v = v;
            it.*.idx += 1;
            return true;
        }

        pub fn reset(it: *Iterator) void {
            it.*.idx = 0;
        }
    };

    pub fn iterator(self: *Self) Iterator {
        return .{
            .idx = 0,
            .k = 0,
            .v = undefined,
            .data = self,
        };
    }
};

test TableMap {
    const testing = std.testing;

    const tm = try TableMap.init(testing.allocator);
    var key: u64 = std.hash.Murmur2_64.hash("__key__");
    const value = "__value__";

    try tm.put(key, value);
    if (tm.get(key)) |actual| {
        try testing.expect(std.mem.eql(u8, actual, "__value__"));
    } else |_| {
        unreachable;
    }

    tm.deinit();
    testing.allocator.destroy(tm);
}
