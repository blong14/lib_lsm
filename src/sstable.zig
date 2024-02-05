const std = @import("std");

const MMap = @import("mmap.zig").MMap;

const Allocator = std.mem.Allocator;

pub const SSTableRow = extern struct {
    key: u64,
    value: [*c]const u8,
};

pub const SSTable = struct {
    alloc: Allocator,
    capacity: usize,
    data: MMap(SSTableRow),

    const Self = @This();

    const Error = error{
        NotFound,
    } || MMap(SSTableRow).Error;

    pub fn init(alloc: Allocator, path: []const u8, capacity: usize) Error!*Self {
        var data = try MMap(SSTableRow).init(path, capacity);
        const cap = capacity / @sizeOf(SSTableRow);
        const st = try alloc.create(Self);
        st.* = .{ .alloc = alloc, .capacity = cap, .data = data };
        return st;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.* = undefined;
    }

    fn findIndex(self: *Self, key: u64, low: usize, high: usize) !usize {
        if (high < low) {
            return high + 1;
        }
        const mid = (low + high) / 2;
        const entry = try self.data.read(mid);
        if (key < entry.key) {
            return self.findIndex(key, low, mid - 1);
        } else if (key == entry.key) {
            return mid;
        } else {
            return self.findIndex(key, mid + 1, high);
        }
    }

    fn equalto(self: *Self, key: u64, idx: usize) bool {
        const entry = self.data.read(idx) catch |err| {
            std.debug.print("Oops {s}\n", .{@errorName(err)});
            return false;
        };
        return key == entry.key;
    }

    fn greaterthan(self: *Self, key: u64, idx: usize) bool {
        const entry = self.data.read(idx) catch |err| {
            std.debug.print("Oops {s}\n", .{@errorName(err)});
            return false;
        };
        return key > entry.key;
    }

    pub fn read(self: *Self, key: u64) anyerror![]const u8 {
        const count = self.data.count;
        if (count == 0) return Error.NotFound;
        const idx = try self.findIndex(key, 0, count - 1);
        if ((idx == -1) or (idx == count)) return Error.NotFound;
        const row = try self.data.read(idx);
        return std.mem.span(row.value);
    }

    pub fn write(self: *Self, key: u64, value: []const u8) Error!void {
        const count: usize = self.data.count;
        if ((count == 0) or (self.greaterthan(key, count - 1))) {
            return try self.data.append(SSTableRow{ .key = key, .value = value.ptr });
        }
        const idx = try self.findIndex(key, 0, count - 1);
        try self.data.insert(idx, SSTableRow{ .key = key, .value = value.ptr });
    }
};

test "SSTable" {
    const testing = std.testing;

    var alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    // given
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "sstable.dat" });
    defer alloc.free(filename);
    var st = try SSTable.init(alloc, filename, std.mem.page_size);
    defer alloc.destroy(st);
    defer st.deinit();

    // when
    const key: u64 = std.hash.Murmur2_64.hash("__key__");
    const expected: []const u8 = "__value__";
    try st.write(key, expected);

    // then
    const actual = try st.read(key);
    try testing.expect(std.mem.eql(u8, expected, actual));
}
