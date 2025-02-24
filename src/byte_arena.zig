const std = @import("std");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Atomic = std.atomic.Value;
const Mutex = std.Thread.Mutex;

pub const ThreadSafeBumpAllocator = struct {
    chunk_size: usize,
    chunks: ArrayList([]u8),
    free_chunks: ArrayList([]u8),
    current_chunk: []u8,
    used: Atomic(usize),
    total_allocations: Atomic(usize),
    total_used_bytes: Atomic(usize),
    alloc: Allocator,
    lock: Mutex,

    pub fn init(alloc: Allocator, initial_chunk_size: usize) !ThreadSafeBumpAllocator {
        var chunks = ArrayList([]u8).init(alloc);
        const free_chunks = ArrayList([]u8).init(alloc);

        const first_chunk = try alloc.alloc(u8, initial_chunk_size);
        try chunks.append(first_chunk);

        return ThreadSafeBumpAllocator{
            .chunk_size = initial_chunk_size,
            .chunks = chunks,
            .free_chunks = free_chunks,
            .current_chunk = first_chunk,
            .used = Atomic(usize).init(0),
            .total_allocations = Atomic(usize).init(0),
            .total_used_bytes = Atomic(usize).init(0),
            .alloc = alloc,
            .lock = Mutex{},
        };
    }

    pub fn deinit(self: *ThreadSafeBumpAllocator) void {
        self.lock.lock();
        defer self.lock.unlock();

        for (self.chunks.items) |chunk| {
            self.alloc.free(chunk);
        }
        self.chunks.deinit();
        self.free_chunks.deinit();
    }

    pub fn allocator(self: *ThreadSafeBumpAllocator) Allocator {
        return Allocator{
            .ptr = self,
            .vtable = &.{
                .alloc = allocFn,
                .resize = resizeFn,
                .free = freeFn,
            },
        };
    }

    fn add_chunk(self: *ThreadSafeBumpAllocator) !void {
        self.lock.lock();
        defer self.lock.unlock();

        var new_chunk: []u8 = undefined;
        if (self.free_chunks.popOrNull()) |reusable| {
            new_chunk = reusable;
        } else {
            new_chunk = try self.alloc.alloc(u8, self.chunk_size * 2);
            self.chunk_size *= 2;
            _ = self.total_allocations.fetchAdd(1, .seq_cst);
        }

        try self.chunks.append(new_chunk);
        self.current_chunk = new_chunk;
        self.used.store(0, .seq_cst);
    }

    fn allocFn(ctx: *anyopaque, len: usize, log2_align: u8, ret_addr: usize) ?[*]u8 {
        const self: *ThreadSafeBumpAllocator = @ptrCast(@alignCast(ctx));
        const algn = @as(usize, 1) << @intCast(log2_align);

        var aligned_start = self.used.load(.seq_cst);
        if (aligned_start % algn != 0) {
            aligned_start += algn - (aligned_start % algn);
        }

        if (aligned_start + len > self.current_chunk.len) {
            self.add_chunk() catch return null;
            return allocFn(ctx, len, log2_align, ret_addr);
        }

        self.used.store(aligned_start + len, .seq_cst);
        _ = self.total_used_bytes.fetchAdd(len, .seq_cst);
        return self.current_chunk.ptr + aligned_start;
    }

    fn resizeFn(ctx: *anyopaque, buf: []u8, log2_align: u8, new_len: usize, ret_addr: usize) bool {
        _ = ctx;
        _ = buf;
        _ = log2_align;
        _ = new_len;
        _ = ret_addr;
        return false; // Does not support resizing individual allocations
    }

    fn freeFn(ctx: *anyopaque, buf: []u8, log2_align: u8, ret_addr: usize) void {
        _ = ret_addr;
        _ = log2_align;
        const self: *ThreadSafeBumpAllocator = @ptrCast(@alignCast(ctx));

        self.lock.lock();
        defer self.lock.unlock();

        for (self.chunks.items, 0..) |*chunk, i| {
            if (chunk.ptr == buf.ptr) {
                _ = self.chunks.swapRemove(i);
                self.free_chunks.append(chunk.*) catch {};
                return;
            }
        }
    }

    pub fn printStats(self: *ThreadSafeBumpAllocator) void {
        std.debug.print(
            "Arena Stats: Allocations={}, UsedBytes={}, Chunks={}, FreeChunks={}\n",
            .{ self.total_allocations.load(.seq_cst), self.total_used_bytes.load(.seq_cst), self.chunks.items.len, self.free_chunks.items.len },
        );
    }
};

fn benchmark(alloc: Allocator, iterations: usize, alloc_size: usize) u64 {
    const start = std.time.milliTimestamp();
    for (0..iterations) |_| {
        const mem = alloc.alloc(u8, alloc_size) catch continue;
        alloc.free(mem);
    }
    return std.time.milliTimestamp() - start;
}

//pub fn main() !void {
//    var gpa = std.heap.page_allocator;
//    var arena_alloc = try ThreadSafeBumpAllocator.init(gpa, 1024); // Start with 1KB chunks
//    defer arena_alloc.deinit();
//
//    var alloc = arena_alloc.allocator();
//
//    const iterations = 100_000;
//    const alloc_size = 64;
//
//    const arena_time = benchmark(alloc, iterations, alloc_size);
//    const gpa_time = benchmark(gpa, iterations, alloc_size);
//
//    std.debug.print("Arena Allocator Time: {} ms\n", .{arena_time});
//    std.debug.print("GeneralPurposeAllocator Time: {} ms\n", .{gpa_time});
//}
