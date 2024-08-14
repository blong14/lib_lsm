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
        const kv = try alloc.create(Self);
        Self.initFields(kv, key, value);
        return kv;
    }

    pub fn initFields(kv: *KV, key: []const u8, value: []const u8) void {
        const len = @sizeOf(u64) + @sizeOf(u64) + key.len + @sizeOf(u64) + value.len;
        kv.* = .{
            .hash = 0,
            .key = key,
            .value = value,
            .len = len,
        };
    }

    pub fn order(a: []const u8, b: KV) Order {
        return std.mem.order(u8, a, b.key);
    }

    pub fn decode(self: *Self, data: []const u8) !void {
        var stream = std.io.fixedBufferStream(data);

        var data_reader = stream.reader();
        self.*.hash = try data_reader.readInt(u64, std.builtin.Endian.little);

        const key_len = try data_reader.readInt(u64, std.builtin.Endian.little);
        assert(key_len > 0);

        self.*.key = stream.buffer[stream.pos..][0..key_len];

        stream.pos += key_len;

        const value_len = try data_reader.readInt(u64, std.builtin.Endian.little);
        assert(value_len > 0);

        self.*.value = stream.buffer[stream.pos..][0..value_len];

        self.*.len = data.len;
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
        try data_writer.writeInt(u64, self.key.len, std.builtin.Endian.little);
        _ = try data_writer.write(self.key);
        try data_writer.writeInt(u64, self.value.len, std.builtin.Endian.little);
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

test "KV decode" {
    const alloc = std.testing.allocator;

    // given
    const expected = try KV.init(alloc, "__key__", "__value__");
    defer alloc.destroy(expected);

    const byts = "\x00\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__";

    // when
    var actual: KV = undefined;
    try actual.decode(byts[0..]);

    // then
    try std.testing.expectEqualStrings(expected.key, actual.key);
    try std.testing.expectEqualStrings(expected.value, actual.value);
    try std.testing.expectEqual(expected.len, actual.len);
}
