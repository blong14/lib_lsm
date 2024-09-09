const std = @import("std");
const msgpack = @import("msgpack");

const Allocator = std.mem.Allocator;
const Order = std.math.Order;
const Payload = msgpack.Payload;

const Endian = std.builtin.Endian.little;

const assert = std.debug.assert;

pub const KV = struct {
    key: []const u8,
    value: []const u8,

    const Self = @This();

    const Error = error{
        OutOfMemory,
    };

    pub fn init(key: []const u8, value: []const u8) Self {
        return .{
            .key = key,
            .value = value,
        };
    }

    pub fn deinit(self: *Self) void {
        self.* = undefined;
    }

    pub fn len(self: Self) u64 {
        return @sizeOf(u64) + self.key.len + @sizeOf(u64) + self.value.len;
    }

    pub fn order(a: []const u8, b: *const KV) Order {
        return std.mem.order(u8, a, b.key);
    }

    pub fn decode(self: *Self, data: []const u8) !void {
        var stream = std.io.fixedBufferStream(data);
        var data_reader = stream.reader();

        const key_len = try data_reader.readInt(u64, Endian);
        assert(key_len > 0);

        const key = stream.buffer[stream.pos..][0..key_len];

        stream.pos += key_len;

        const value_len = try data_reader.readInt(u64, Endian);
        assert(value_len > 0);

        const value = stream.buffer[stream.pos..][0..value_len];

        self.*.key = key;
        self.*.value = value;
    }

    pub fn encodeAlloc(self: Self, alloc: Allocator) ![]const u8 {
        const buf = try alloc.alloc(u8, self.len());
        return try self.encode(buf);
    }

    pub fn encode(self: Self, buf: []u8) ![]u8 {
        const len_ = self.len();
        assert(len_ <= buf.len);

        var stream = std.io.fixedBufferStream(buf);
        var data_writer = stream.writer();

        try data_writer.writeInt(u64, self.key.len, Endian);
        _ = try data_writer.write(self.key);
        try data_writer.writeInt(u64, self.value.len, Endian);
        _ = try data_writer.write(self.value);

        return buf[0..len_];
    }

    pub fn toPayload(self: Self, alloc: Allocator) !Payload {
        var out = Payload.mapPayload(alloc);
        try out.mapPut(self.key, try Payload.strToPayload(self.value, alloc));

        return out;
    }
};

test "KV encode alloc" {
    const alloc = std.testing.allocator;

    // given
    const kv = KV.init("__key__", "__value__");

    // when
    const str = try kv.encodeAlloc(alloc);
    defer alloc.free(str);

    // then
    try std.testing.expectEqualSlices(
        u8,
        "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
        str,
    );
}

test "KV encode" {
    // given
    const kv = KV.init("__key__", "__value__");

    // when
    var buf: [std.mem.page_size]u8 = undefined;
    const str = try kv.encode(&buf);

    // then
    try std.testing.expectEqualSlices(
        u8,
        "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
        str,
    );
}

test "KV decode" {
    // given
    const expected = KV.init("__key__", "__value__");

    const byts = "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__";

    // when
    var actual: KV = undefined;
    try actual.decode(byts[0..]);

    // then
    try std.testing.expectEqualStrings(expected.key, actual.key);
    try std.testing.expectEqualStrings(expected.value, actual.value);
    try std.testing.expectEqual(expected.len(), actual.len());
}
