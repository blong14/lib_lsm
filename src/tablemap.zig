const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn TableMap(comptime V: type) type {
    return struct {
        const Self = @This();

        pub const MapEntry = struct {
            key: u64,
            value: V,
        };

        const TableMapError = error{
            Empty,
            NotFound,
            OutOfMemory,
        };

        const MapEntryTable = std.MultiArrayList(MapEntry);

        gpa: Allocator,
        impl: *MapEntryTable,

        pub fn init(alloc: Allocator) !*Self {
            var impl = try alloc.create(MapEntryTable);
            impl.* = MapEntryTable{};
            const tm = try alloc.create(Self);
            tm.* = .{ .gpa = alloc, .impl = impl };
            return tm;
        }

        pub fn deinit(self: *Self) void {
            self.impl.deinit(self.gpa);
            self.gpa.destroy(self.impl);
            self.* = undefined;
        }

        pub fn findIndex(self: Self, key: u64, low: usize, high: usize) usize {
            var start = low;
            var end = high;
            while (start < end) {
                const mid: usize = start + (end - start) / 2;
                const entry = self.impl.items(.key)[mid];
                if (key < entry) {
                    end = mid;
                } else if (key > entry) {
                    start = mid + 1;
                } else {
                    return mid;
                }
            }
            return start;
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

        pub fn get(self: Self, k: u64) TableMapError!V {
            const cnt = self.count();
            if (cnt == 0) return TableMapError.NotFound;
            const idx = self.findIndex(k, 0, cnt - 1);
            if ((idx == cnt) or !self.equalto(k, idx)) {
                return TableMapError.NotFound;
            }
            return self.impl.get(idx).value;
        }

        pub fn put(self: *Self, k: u64, v: V) TableMapError!void {
            const cnt: usize = self.count();
            if ((cnt == 0) or (self.greaterthan(k, cnt - 1))) {
                return try self.impl.append(self.gpa, MapEntry{ .key = k, .value = v });
            }
            const idx = self.findIndex(k, 0, cnt - 1);
            try self.impl.insert(self.gpa, idx, MapEntry{ .key = k, .value = v });
        }

        pub fn append(self: *Self, k: u64, v: V) !void {
            return try self.impl.append(self.gpa, MapEntry{ .key = k, .value = v });
        }

        const ScanIterator = struct {
            end: u64,
            end_idx: usize,
            idx: usize,
            last_returned: ?MapEntry,
            mtable: *Self,
            nxt: ?MapEntry,

            pub fn advance(it: *ScanIterator, r: ?MapEntry) !void {
                var nxt: ?MapEntry = null;
                it.last_returned = r;
                if (it.last_returned != null) {
                    const cnt = it.mtable.count();
                    if (cnt > 0) {
                        it.idx += 1;
                        nxt = it.mtable.getEntryByIdx(it.idx) catch null;
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

        pub fn scanner(self: *Self, start: u64, end: u64) TableMapError!ScanIterator {
            const cnt = self.count();
            if (cnt == 0) return TableMapError.Empty;
            const start_idx: usize = self.findIndex(start, 0, cnt - 1);
            var end_idx: usize = self.findIndex(end, 0, cnt - 1);
            _ = self.getEntryByIdx(end_idx) catch {
                end_idx = cnt - 1;
            };
            var sc: ScanIterator = .{
                .idx = start_idx,
                .last_returned = null,
                .mtable = self,
                .nxt = null,
                .end_idx = end_idx,
                .end = end,
            };
            const entry = self.impl.get(sc.idx);
            try sc.advance(entry);
            return sc;
        }

        pub const Iterator = struct {
            idx: usize,
            k: u64,
            v: V,
            data: *Self,

            pub fn key(it: Iterator) u64 {
                return it.k;
            }

            pub fn value(it: Iterator) V {
                return it.v;
            }

            pub fn next(it: *Iterator) bool {
                if (it.*.idx >= it.data.count()) return false;
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

        pub const ReverseIterator = struct {
            idx: usize,
            k: u64,
            v: V,
            data: *Self,

            pub fn key(it: ReverseIterator) u64 {
                return it.k;
            }

            pub fn value(it: ReverseIterator) V {
                return it.v;
            }

            pub fn next(it: *ReverseIterator) bool {
                const entry = it.data.impl.get(it.*.idx);
                it.*.k = entry.key;
                it.*.v = entry.value;
                if (it.*.idx == 0) return false;
                it.*.idx -= 1;
                return true;
            }

            pub fn reset(it: *ReverseIterator) void {
                var start_idx: usize = it.impl.len;
                if (start_idx > 0) {
                    start_idx -= 1;
                }
                it.*.idx = start_idx;
            }
        };

        pub fn reverseIterator(self: *Self) ReverseIterator {
            var start_idx: usize = self.impl.len;
            if (start_idx > 0) {
                start_idx -= 1;
            }
            return .{
                .idx = start_idx,
                .k = 0,
                .v = undefined,
                .data = self,
            };
        }
    };
}

test TableMap {
    const testing = std.testing;

    const tm = try TableMap([]const u8).init(testing.allocator);
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

    const tm = try TableMap([]const u8).init(testing.allocator);
    defer testing.allocator.destroy(tm);
    defer tm.deinit();

    const value = "__value__";
    var first: u64 = std.hash.Murmur2_64.hash("aa");
    var second: u64 = std.hash.Murmur2_64.hash("bb");
    var third: u64 = std.hash.Murmur2_64.hash("cc");

    try tm.put(first, value);
    try tm.put(second, value);
    try tm.put(third, value);

    var items = std.ArrayList(u64).init(testing.allocator);
    defer items.deinit();

    var scnr = try tm.scanner(first, third);
    while (scnr.hasNext()) {
        if (try scnr.next()) |entry| {
            try items.append(entry.key);
        }
    }

    std.debug.print("{d}\n", .{items.items.len});
    try testing.expect(items.items.len == 2);
}
