const std = @import("std");

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

test Iterator {
    const testing = std.testing;

    const alloc = testing.allocator;

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
