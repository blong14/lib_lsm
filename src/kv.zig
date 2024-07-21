const std = @import("std");

const Allocator = std.mem.Allocator;
const Order = std.math.Order;

pub const KV = struct {
    hash: u64,
    key: []const u8,
    value: []const u8,

    const Self = @This();

    pub fn order(a: []const u8, b: KV) Order {
        return std.mem.order(u8, a, b.key);
    }

    pub fn encode(self: Self, alloc: Allocator) ![]const u8 {
        var buf = try alloc.alloc(u8, self.key.len + self.value.len);
        @memcpy(buf[0..self.key.len], self.key);
        @memcpy(buf[self.key.len..], self.value);
        return buf;
    }
};

test "KV" {
    // given
    const alloc = std.testing.allocator;
    const kv: KV = .{
        .hash = 0,
        .key = "__key__",
        .value = "__value__",
    };

    // when
    const str = try kv.encode(alloc);
    defer alloc.free(str);

    // then
    try std.testing.expectEqualStrings("__key____value__", str);
}
