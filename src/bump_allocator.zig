const std = @import("std");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Atomic = std.atomic.Value;
const Mutex = std.Thread.Mutex;

pub const ThreadSafeBumpAllocator = struct {
    alloc: Allocator,
    chunk_size: usize,
    chunks: ArrayList([]align(16) u8),
    current_chunk: []align(16) u8,
    used: Atomic(usize),
    total_allocations: Atomic(usize),
    total_used_bytes: Atomic(usize),
    lock: Mutex,

    pub fn init(alloc: Allocator, initial_chunk_size: usize) !ThreadSafeBumpAllocator {
        var chunks = ArrayList([]align(16) u8).init(alloc);

        // Ensure initial chunk size is at least 4KB and a power of 2
        const adjusted_size = std.math.ceilPowerOfTwo(usize, @max(initial_chunk_size, 4096)) catch initial_chunk_size;

        const first_chunk = try alloc.alignedAlloc(u8, 16, adjusted_size);
        errdefer alloc.free(first_chunk);

        try chunks.append(first_chunk);

        return ThreadSafeBumpAllocator{
            .chunk_size = adjusted_size,
            .chunks = chunks,
            .current_chunk = first_chunk,
            .used = Atomic(usize).init(0),
            // Count the first chunk
            .total_allocations = Atomic(usize).init(1),
            .total_used_bytes = Atomic(usize).init(0),
            .alloc = alloc,
            .lock = Mutex{},
        };
    }

    pub fn deinit(self: *ThreadSafeBumpAllocator) void {
        self.lock.lock();
        defer self.lock.unlock();

        // Free all chunks in the active list
        for (self.chunks.items) |chunk| {
            self.alloc.free(chunk);
        }
        self.chunks.deinit();
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
        // Double chunk size for exponential growth, but cap at 1MB to avoid
        // excessive memory usage
        const new_size = @min(self.chunk_size * 2, 1024 * 1024);
        const new_chunk = try self.alloc.alignedAlloc(u8, 16, new_size);

        if (new_size > self.chunk_size) {
            self.chunk_size = new_size;
        }

        try self.chunks.append(new_chunk);
        self.current_chunk = new_chunk;

        self.used.store(0, .monotonic);
        _ = self.total_allocations.fetchAdd(1, .monotonic);
    }

    fn allocFn(ctx: *anyopaque, len: usize, log2_align: u8, ret_addr: usize) ?[*]u8 {
        const self: *ThreadSafeBumpAllocator = @ptrCast(@alignCast(ctx));
        const algn = @as(usize, 1) << @intCast(log2_align);

        // Fast path for small allocations with common alignments
        if (len <= 64 and algn <= 16) {
            return self.fastAllocate(len, algn);
        }

        // Slow path for larger or unusually aligned allocations
        return self.slowAllocate(len, algn, ret_addr);
    }

    fn fastAllocate(self: *ThreadSafeBumpAllocator, len: usize, algn: usize) ?[*]u8 {
        const current_used = self.used.load(.acquire);
        const current_chunk = self.current_chunk;

        const aligned_start = std.mem.alignForward(usize, current_used, algn);
        const new_used = aligned_start + len;

        if (new_used <= current_chunk.len) {
            if (self.used.cmpxchgWeak(current_used, new_used, .acq_rel, .acquire)) |_| {
                return self.slowAllocate(len, algn, 0);
            }

            // We successfully claimed the memory
            _ = self.total_used_bytes.fetchAdd(len, .monotonic);

            return current_chunk.ptr + aligned_start;
        }

        return self.slowAllocate(len, algn, 0);
    }

    fn slowAllocate(self: *ThreadSafeBumpAllocator, len: usize, algn: usize, ret_addr: usize) ?[*]u8 {
        _ = ret_addr;

        self.lock.lock();
        defer self.lock.unlock();

        var current_chunk = self.current_chunk;
        var aligned_start = std.mem.alignForward(usize, self.used.load(.acquire), algn);

        if (aligned_start + len > current_chunk.len) {
            self.add_chunk() catch return null;

            current_chunk = self.current_chunk;
            aligned_start = 0;

            if (len > current_chunk.len) {
                // Requested allocation is larger than our chunk size
                // Allocate a dedicated chunk for this request
                const large_chunk = self.alloc.alignedAlloc(u8, 16, len) catch return null;
                self.chunks.append(large_chunk) catch {
                    self.alloc.free(large_chunk);
                    return null;
                };

                _ = self.total_allocations.fetchAdd(1, .monotonic);
                _ = self.total_used_bytes.fetchAdd(len, .monotonic);

                return large_chunk.ptr;
            }
        }

        self.used.store(aligned_start + len, .monotonic);
        _ = self.total_used_bytes.fetchAdd(len, .monotonic);

        return current_chunk.ptr + aligned_start;
    }

    fn resizeFn(ctx: *anyopaque, buf: []u8, log2_align: u8, new_len: usize, ret_addr: usize) bool {
        _ = log2_align;
        _ = ret_addr;

        const self: *ThreadSafeBumpAllocator = @ptrCast(@alignCast(ctx));

        const current_chunk = self.current_chunk;
        const chunk_start = @intFromPtr(current_chunk.ptr);
        const buf_end = @intFromPtr(buf.ptr) + buf.len;
        const current_used = self.used.load(.acquire);

        if (buf_end == chunk_start + current_used and
            chunk_start + current_used + (new_len - buf.len) <= chunk_start + current_chunk.len)
        {
            self.used.store(current_used + (new_len - buf.len), .monotonic);
            _ = self.total_used_bytes.fetchAdd(new_len - buf.len, .monotonic);

            return true;
        }

        return false;
    }

    fn freeFn(ctx: *anyopaque, buf: []u8, log2_align: u8, ret_addr: usize) void {
        _ = ret_addr;
        _ = log2_align;

        const self: *ThreadSafeBumpAllocator = @ptrCast(@alignCast(ctx));

        const current_chunk = self.current_chunk;
        const buf_start = @intFromPtr(buf.ptr);
        const chunk_start = @intFromPtr(current_chunk.ptr);
        const current_used = self.used.load(.acquire);

        if (buf_start + buf.len == chunk_start + current_used and
            buf_start >= chunk_start and buf_start < chunk_start + current_chunk.len)
        {
            const new_used = buf_start - chunk_start;

            self.used.store(new_used, .monotonic);
            _ = self.total_used_bytes.fetchSub(buf.len, .monotonic);

            return;
        }
    }

    pub fn printStats(self: *ThreadSafeBumpAllocator) void {
        self.lock.lock();
        defer self.lock.unlock();

        std.log.info(
            "Arena Stats: Allocations={}, UsedBytes={}, Chunks={}\n",
            .{
                self.total_allocations.load(.monotonic),
                self.total_used_bytes.load(.monotonic),
                self.chunks.items.len,
            },
        );
    }
};

fn benchmark(alloc: Allocator, iterations: usize, alloc_size: usize) i64 {
    // Warm up the allocator
    for (0..1000) |_| {
        const mem = alloc.alloc(u8, alloc_size) catch continue;
        alloc.free(mem);
    }

    const start = std.time.milliTimestamp();

    // Use an array to store allocations to prevent optimizing away the allocations
    var allocations: [1000][]u8 = undefined;
    var alloc_count: usize = 0;

    for (0..iterations) |i| {
        const mem = alloc.alloc(u8, alloc_size) catch continue;

        mem[0] = @truncate(i);

        if (i % 100 == 0 and alloc_count < allocations.len) {
            allocations[alloc_count] = mem;
            alloc_count += 1;
        } else {
            alloc.free(mem);
        }
    }

    for (0..alloc_count) |i| {
        alloc.free(allocations[i]);
    }

    return std.time.milliTimestamp() - start;
}

test "Benchmark" {
    const testing = std.testing;

    const talloc = testing.allocator;
    const calloc = std.heap.c_allocator;

    var bump = try ThreadSafeBumpAllocator.init(talloc, 64 * 1024);
    defer bump.deinit();

    const bump_alloc = bump.allocator();

    const iterations = 100_000;
    const alloc_size = 64;

    var std_total: i64 = 0;
    var bump_total: i64 = 0;
    var test_total: i64 = 0;

    // Run multiple times to get more stable results
    const runs: i64 = 3;
    for (0..runs) |_| {
        const std_time = benchmark(calloc, iterations, alloc_size);
        const bump_time = benchmark(bump_alloc, iterations, alloc_size);
        const test_time = benchmark(talloc, iterations, alloc_size);

        std_total += std_time;
        bump_total += bump_time;
        test_total += test_time;
    }

    const std_time = @divFloor(std_total, runs);
    const bump_time = @divFloor(bump_total, runs);
    const test_time = @divFloor(test_total, runs);

    std.debug.print("\nBenchmark Results (avg of {} runs):\n", .{runs});
    std.debug.print("C allocator: {} ms\n", .{std_time});
    std.debug.print("Bump allocator: {} ms\n", .{bump_time});
    std.debug.print("Test allocator: {} ms\n", .{test_time});
    std.debug.print("Speedup (bump vs c): {d:.2}x\n", .{@as(f64, @floatFromInt(std_time)) / @as(f64, @floatFromInt(bump_time))});

    std.debug.print("\nAllocation size comparison:\n", .{});

    const sizes = [_]usize{ 8, 32, 128, 512, 1024, 2046 };
    for (sizes) |size| {
        const std_size_time = benchmark(calloc, iterations / 4, size);
        const bump_size_time = benchmark(bump_alloc, iterations / 4, size);
        const test_size_time = benchmark(talloc, iterations / 4, size);

        std.debug.print(
            "Size {}: C={} ms, Bump={} ms, Ratio={d:.2}x, Test={} ms\n",
            .{
                size,
                std_size_time,
                bump_size_time,
                @as(f64, @floatFromInt(std_size_time)) / @as(f64, @floatFromInt(bump_size_time)),
                test_size_time,
            },
        );
    }
}
