const std = @import("std");

const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Order = std.math.Order;

pub const KV = struct {
    hash: u64,
    key: []const u8,
    value: []const u8,
    len: u64,

    const Self = @This();

    const Error = error{
        OutOfMemory,
    };

    pub fn init(alloc: Allocator, key: []const u8, value: []const u8) !*Self {
        const len = @sizeOf(u64) + @sizeOf(usize) + key.len + @sizeOf(usize) + value.len;
        const kv = try alloc.create(Self);
        kv.* = .{
            .hash = 0,
            .len = len,
            .key = key,
            .value = value,
        };
        return kv;
    }

    pub fn order(a: []const u8, b: KV) Order {
        return std.mem.order(u8, a, b.key);
    }

    pub fn decode(self: *Self, data: []u8) !void {
        var stream = std.io.fixedBufferStream(data);

        var data_reader = stream.reader();
        self.*.hash = try data_reader.readInt(u64, std.builtin.Endian.little);

        const key_len = try data_reader.readInt(usize, std.builtin.Endian.little);
        assert(key_len > 0);

        var key: [std.mem.page_size]u8 = undefined;
        _ = try data_reader.readAtLeast(key[0..key_len], key_len);
        self.*.key = key[0..key_len];

        const value_len = try data_reader.readInt(usize, std.builtin.Endian.little);
        assert(value_len > 0);

        var value: [std.mem.page_size]u8 = undefined;
        _ = try data_reader.readAtLeast(value[0..value_len], value_len);
        self.*.value = value[0..value_len];
    }

    pub fn encodeAlloc(self: Self, alloc: Allocator) ![]const u8 {
        const buf = try alloc.alloc(u8, self.len);
        return try self.encode(buf);
    }

    pub fn encode(self: Self, buf: []u8) ![]u8 {
        assert(self.len <= buf.len);
        var stream = std.io.fixedBufferStream(buf);

        var data_writer = stream.writer();
        try data_writer.writeInt(u64, self.hash, std.builtin.Endian.little);
        try data_writer.writeInt(usize, self.key.len, std.builtin.Endian.little);
        _ = try data_writer.write(self.key);
        try data_writer.writeInt(usize, self.value.len, std.builtin.Endian.little);
        _ = try data_writer.write(self.value);

        return buf[0..self.len];
    }
};

test "KV encode alloc" {
    const alloc = std.testing.allocator;

    // given
    const kv = try KV.init(alloc, "__key__", "__value__");
    defer alloc.destroy(kv);

    // when
    const str = try kv.encodeAlloc(alloc);
    defer alloc.free(str);

    // then
    try std.testing.expectEqualSlices(
        u8,
        "\x00\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
        str,
    );
}

test "KV encode" {
    const alloc = std.testing.allocator;

    // given
    const kv = try KV.init(alloc, "__key__", "__value__");
    defer alloc.destroy(kv);

    // when
    var buf: [std.mem.page_size]u8 = undefined;
    const str = try kv.encode(&buf);

    // then
    try std.testing.expectEqualSlices(
        u8,
        "\x00\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
        str,
    );
}
