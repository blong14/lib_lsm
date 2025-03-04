const std = @import("std");

const Allocator = std.mem.Allocator;

var ballast: usize = std.mem.page_size;

fn xalloc(alloc: Allocator, buffer: *[]u8, len_: usize) ![]u8 {
    if (buffer.*.len == 0) {
        buffer.* = try alloc.alloc(u8, ballast);
        ballast *= 2;
    }
    const result = @subWithOverflow(buffer.*.len - 1, len_);
    var offset = result[0];
    if (result[1] != 0) {
        buffer.* = try alloc.alloc(u8, ballast);
        ballast *= 2;
        offset = (buffer.*.len - 1) - len_;
    }
    var n: []u8 = undefined;
    n = buffer.*[offset .. buffer.*.len - 1];
    buffer.* = buffer.*[0..offset];
    return n;
}

test "xalloc" {
    const testing = std.testing;
    const alloc = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var buffer = try arena.allocator().alloc(u8, ballast);

    var actual = try xalloc(arena.allocator(), &buffer, 4094);

    try testing.expectEqual(actual.len, 4094);

    actual = try xalloc(arena.allocator(), &buffer, 4094);

    try testing.expectEqual(actual.len, 4094);

    actual = try xalloc(arena.allocator(), &buffer, 2);

    try testing.expectEqual(actual.len, 2);
}
