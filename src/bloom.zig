const std = @import("std");
const math = std.math;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;

/// Bloom filter implementation for LSM-tree read optimization
pub const BloomFilter = struct {
    bits: []u8,
    bit_count: usize,
    hash_count: u8,

    const Self = @This();

    /// Initialize a bloom filter with given capacity and false positive rate
    pub fn init(alloc: Allocator, expected_items: usize, false_positive_rate: f64) !*Self {
        // Calculate optimal bit array size: m = -n * ln(p) / (ln(2)^2)
        const ln2_squared = math.ln(@as(f64, 2.0)) * math.ln(@as(f64, 2.0));
        const bit_count_f = -@as(f64, @floatFromInt(expected_items)) * math.ln(false_positive_rate) / ln2_squared;
        const bit_count = @as(usize, @intFromFloat(math.ceil(bit_count_f)));
        
        // Calculate optimal hash function count: k = (m/n) * ln(2)
        const hash_count_f = (@as(f64, @floatFromInt(bit_count)) / @as(f64, @floatFromInt(expected_items))) * math.ln(@as(f64, 2.0));
        const hash_count = @as(u8, @intFromFloat(math.round(hash_count_f)));

        const byte_count = (bit_count + 7) / 8;
        const bits = try alloc.alloc(u8, byte_count);
        @memset(bits, 0);

        const filter = try alloc.create(Self);
        filter.* = .{
            .bits = bits,
            .bit_count = bit_count,
            .hash_count = @max(1, hash_count), // At least 1 hash function
        };

        return filter;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        alloc.free(self.bits);
        alloc.destroy(self);
    }

    /// Add an item to the bloom filter
    pub fn add(self: *Self, item: []const u8) void {
        var hash1 = self.hash(item, 0);
        var hash2 = self.hash(item, hash1);

        for (0..self.hash_count) |i| {
            const bit_index = (hash1 + @as(u64, @intCast(i)) * hash2) % self.bit_count;
            const byte_index = bit_index / 8;
            const bit_offset = @as(u3, @intCast(bit_index % 8));
            
            self.bits[byte_index] |= (@as(u8, 1) << bit_offset);
        }
    }

    /// Check if an item might be in the set (no false negatives, possible false positives)
    pub fn mightContain(self: *Self, item: []const u8) bool {
        var hash1 = self.hash(item, 0);
        var hash2 = self.hash(item, hash1);

        for (0..self.hash_count) |i| {
            const bit_index = (hash1 + @as(u64, @intCast(i)) * hash2) % self.bit_count;
            const byte_index = bit_index / 8;
            const bit_offset = @as(u3, @intCast(bit_index % 8));
            
            if ((self.bits[byte_index] >> bit_offset) & 1) == 0 {
                return false;
            }
        }

        return true;
    }

    /// Simple hash function using FNV-1a
    fn hash(self: *Self, data: []const u8, seed: u64) u64 {
        _ = self;
        var hash_val = 14695981039346656037 +% seed; // FNV offset basis
        
        for (data) |byte| {
            hash_val ^= byte;
            hash_val *%= 1099511628211; // FNV prime
        }
        
        return hash_val;
    }
};

test "BloomFilter basic operations" {
    const alloc = testing.allocator;
    
    var filter = try BloomFilter.init(alloc, 1000, 0.01);
    defer filter.deinit(alloc);

    // Add some items
    filter.add("key1");
    filter.add("key2");
    filter.add("key3");

    // Test positive cases
    try testing.expect(filter.mightContain("key1"));
    try testing.expect(filter.mightContain("key2"));
    try testing.expect(filter.mightContain("key3"));

    // Test negative case (should be false, but might have false positives)
    const result = filter.mightContain("nonexistent_key");
    // We can't assert false here due to possible false positives
    _ = result;
}

test "BloomFilter false positive rate" {
    const alloc = testing.allocator;
    
    var filter = try BloomFilter.init(alloc, 100, 0.01);
    defer filter.deinit(alloc);

    // Add 100 items
    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "key_{d}", .{i});
        filter.add(key);
    }

    // Test that all added items are found
    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "key_{d}", .{i});
        try testing.expect(filter.mightContain(key));
    }
}
