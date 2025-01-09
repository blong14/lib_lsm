const std = @import("std");

const Allocator = std.mem.Allocator;

arena: std.heap.ArenaAllocator,
block_size: usize = std.mem.page_size,
buf: ?[]u8 = null,
mutex: std.Thread.Mutex = .{},

const Self = @This();

pub fn allocator(self: *Self) Allocator {
    const malloc: Allocator = .{
        .ptr = self,
        .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .free = free,
        },
    };

    self.arena = std.heap.ArenaAllocator.init(malloc);

    return self.arena.allocator();
}

pub fn deinit(self: *Self) void {
    self.arena.deinit();
}

fn alloc(ctx: *anyopaque, n: usize, log2_ptr_align: u8, ra: usize) ?[*]u8 {
    const self: *Self = @ptrCast(@alignCast(ctx));
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.buf.*.len == 0) {
        self.buf.* = self.arena.allocator().rawAlloc(self.block_size, log2_ptr_align, ra);
        self.block_size *= 2;
    }

    const result = @subWithOverflow(self.buf.*.len - 1, n);
    var offset = result[0];
    if (result[1] != 0) {
        self.buf.* = self.arena.allocator().rawAlloc(self.block_size, log2_ptr_align, ra);
        self.block_size *= 2;
        offset = (self.buf.*.len - 1) - n;
    }

    const data = self.buf.*[offset .. self.buf.*.len - 1];
    self.buf.* = self.buf.*[0..offset];

    return data;
}

fn resize(ctx: *anyopaque, buf: []u8, log2_buf_align: u8, new_len: usize, ret_addr: usize) bool {
    _ = ctx;
    _ = buf;
    _ = log2_buf_align;
    _ = new_len;
    _ = ret_addr;

    return true;
}

fn free(ctx: *anyopaque, buf: []u8, log2_buf_align: u8, ret_addr: usize) void {
    _ = ctx;
    _ = buf;
    _ = log2_buf_align;
    _ = ret_addr;
}
