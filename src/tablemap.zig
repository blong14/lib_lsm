const std = @import("std");
const Allocator = std.mem.Allocator;

pub const MapEntry = struct {
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

    pub fn findIndex(self: Self, key: u64, low: usize, high: usize) usize {
        if (high < low) {
            return high + 1;
        }
        const mid = low + ((high - low) / 2);
        const entry = self.impl.items(.key)[mid];
        if (key < entry) {
            if (mid == 0) return mid;
            return self.findIndex(key, low, mid - 1);
        } else if (key == entry) {
            return mid;
        } else {
            return self.findIndex(key, mid + 1, high);
        }
    }

    fn equalto(self: Self, key: u64, idx: usize) bool {
        const entry = self.impl.items(.key)[idx];
        return key == entry;
    }

    fn greaterthan(self: Self, key: u64, idx: usize) bool {
        const entry = self.impl.items(.key)[idx];
        return key > entry;
    }

    pub fn count(self: Self) usize {
        return self.impl.len;
    }

    pub fn getEntryByIdx(self: Self, idx: usize) TableMapError!MapEntry {
        const cnt = self.count();
        if (idx >= cnt) return TableMapError.NotFound;
        return self.impl.get(idx);
    }

    pub fn get(self: Self, k: u64) TableMapError![]const u8 {
        const cnt = self.count();
        if (cnt == 0) return TableMapError.NotFound;
        const idx = self.findIndex(k, 0, cnt - 1);
        if ((idx == cnt) or !self.equalto(k, idx)) {
            return TableMapError.NotFound;
        }
        return self.impl.get(idx).value;
    }

    pub fn put(self: *Self, k: u64, v: []const u8) !void {
        const cnt: usize = self.count();
        if ((cnt == 0) or (self.greaterthan(k, cnt - 1))) {
            return try self.impl.append(self.gpa, MapEntry{ .key = k, .value = v });
        }
        const idx = self.findIndex(k, 0, cnt - 1);
        try self.impl.insert(self.gpa, idx, MapEntry{ .key = k, .value = v });
    }

    const ScanIterator = struct {
        mtable: *TableMap,
        last_returned: ?MapEntry,
        nxt: ?MapEntry,
        start: u64,
        end: u64,

        pub fn advance(it: *ScanIterator, r: ?MapEntry) !void {
            var nxt: ?MapEntry = null;
            it.last_returned = r;
            if (it.last_returned != null) {
                const cnt = it.mtable.count();
                if (cnt > 0) {
                    const idx = it.mtable.findIndex(it.last_returned.?.key, 0, cnt - 1);
                    nxt = it.mtable.getEntryByIdx(idx + 1) catch null;
                }
            }
            if (nxt != null and it.start > nxt.?.key) {
                const cnt = it.mtable.count();
                if (cnt > 0) {
                    const start_idx = it.mtable.findIndex(it.start, 0, cnt - 1);
                    nxt = it.mtable.getEntryByIdx(start_idx) catch null;
                }
            }
            it.*.nxt = nxt;
        }

        pub fn hasNext(it: *ScanIterator) bool {
            if (it.*.nxt == null) return false;
            return it.*.nxt.?.key <= it.end;
        }

        pub fn next(it: *ScanIterator) TableMapError!?MapEntry {
            const current = it.nxt;
            try it.advance(current);
            return current;
        }
    };

    pub fn scanner(self: *Self, start: u64, end: u64) !ScanIterator {
        var sc: ScanIterator = .{
            .last_returned = null,
            .mtable = self,
            .nxt = null,
            .start = start,
            .end = end,
        };
        const entry = self.impl.get(0);
        try sc.advance(entry);
        return sc;
    }

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
            const entry = it.data.impl.get(it.*.idx);
            it.*.k = entry.key;
            it.*.v = entry.value;
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

test "TableMap Scan" {
    const testing = std.testing;

    const tm = try TableMap.init(testing.allocator);
    defer testing.allocator.destroy(tm);
    defer tm.deinit();

    const value = "__value__";
    var first: u64 = std.hash.Murmur2_64.hash("aa");
    var second: u64 = std.hash.Murmur2_64.hash("bb");
    var third: u64 = std.hash.Murmur2_64.hash("cc");

    try tm.put(first, value);
    try tm.put(second, value);
    try tm.put(third, value);
    std.debug.print("{d}\n", .{tm.count()});

    var items = std.ArrayList(MapEntry).init(testing.allocator);
    defer items.deinit();

    var scnr = try tm.scanner(first, third);
    while (scnr.hasNext()) {
        if (try scnr.next()) |entry| {
            try items.append(entry);
        }
    }

    std.debug.print("{d}\n", .{items.items.len});
    try testing.expect(items.items.len == 2);
}
