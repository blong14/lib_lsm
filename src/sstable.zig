const std = @import("std");

const AppendOnlyMMap = @import("mmap.zig").AppendOnlyMMap;
const KV = @import("kv.zig").KV;
const MMap = @import("mmap.zig").MMap;
const TableMap = @import("tablemap.zig").TableMap;
const options = @import("opts.zig");

const Allocator = std.mem.Allocator;
const FixedBuffer = std.io.FixedBufferStream;
const Opts = options.Opts;

pub const Block = struct {
    capacity: usize,
    data: FixedBuffer([]align(std.mem.page_size) u8),
    offset: FixedBuffer([]align(std.mem.page_size) u8),
    count: u16,

    const Self = @This();

    const Error = error{
        ReadError,
        WriteError,
    };

    pub fn init(alloc: Allocator) !*Self {
        const capacity = std.mem.page_size;
        const data = try alloc.alloc(u8, capacity);
        const offset = try alloc.alloc(u8, capacity);

        const block = try alloc.create(Self);
        block.* = .{
            .capacity = capacity,
            .data = std.io.fixedBufferStream(@as([]align(std.mem.page_size) u8, @alignCast(data))),
            .offset = std.io.fixedBufferStream(@as([]align(std.mem.page_size) u8, @alignCast(offset))),
            .count = 0,
        };
        return block;
    }

    pub fn deinit(self: *Self) void {
        self.* = undefined;
    }

    pub fn read(self: Self, offset: u16, kv: *KV) !void {
        try self.data.seekTo(offset);

        const data_reader = self.data.reader();
        kv.*.hash = try data_reader.readInt(u64, std.builtin.Endian.little);

        const key_len = try data_reader.readInt(u16, std.builtin.Endian.little);
        const key = try self.alloc.alloc(u8, key_len);
        try data_reader.readAtLeast(key, key_len);
        kv.*.key = key;

        const value_len = try data_reader.readInt(u16, std.builtin.Endian.little);
        const value = try self.alloc.alloc(u8, value_len);
        try data_reader.readAtLeast(value, value_len);
        kv.*.value = value;
    }

    pub fn write(self: *Self, kv: KV) !usize {
        const offset = try self.data.getPos();
        const end = try self.data.getEndPos();
        const size = @sizeOf(u64) + @sizeOf(u16) + kv.key.len + @sizeOf(u16) + kv.value.len;
        const remaining = end - offset;
        if (size > remaining) {
            std.debug.print("ooops {d} {d}\n", .{ size, remaining });
            return error.WriteError;
        }

        const data_writer = self.data.writer();
        try data_writer.writeInt(u64, kv.hash, std.builtin.Endian.little);
        try data_writer.writeInt(usize, kv.key.len, std.builtin.Endian.little);
        _ = try data_writer.write(kv.key);
        try data_writer.writeInt(usize, kv.value.len, std.builtin.Endian.little);
        _ = try data_writer.write(kv.value);

        const offset_writer = self.offset.writer();
        try offset_writer.writeInt(usize, offset, std.builtin.Endian.little);

        self.count += 1;
        return offset;
    }

    /// [count, offset1, offset2, kv[hash, keylen, key, valuelen, value], kv[hash, keylen, key, valuelen, value]]
    pub fn flush(self: Self, outFile: std.io.File) !void {
        var count: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        std.mem.writeInt(std.math.ByteAlignedInt(u64), &count, self.count, std.builtin.Endian.little);

        const buf = outFile.writer();
        try buf.write(count);
        try buf.write(self.offset.getWritten());
        try buf.write(self.data.getWritten());
    }
};

pub const SSTableRow = extern struct {
    key: u64,
    value: [*c]const u8,
};

pub const SSTable = struct {
    block: *Block,
    capacity: usize,
    connected: bool,
    file: std.fs.File,
    id: u64,
    mutable: bool,

    const Self = @This();

    /// [meta,data]
    /// [meta[len,count,version],block[count,offset1,offset2,kv1,kv2],block[count,offset1,offset2,kv1,kv2]]
    const TableMeta = struct {
        len: u64,
        count: usize,
        version: u16,
    };

    const TableData = struct {
        count: u64,
        klen: u16,
        key: []const u8,
        vlen: u16,
        value: []const u8,
    };

    const Error = error{
        NotFound,
    } || AppendOnlyMMap.Error;

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity / @sizeOf(SSTableRow);
        const block = try Block.init(alloc);

        const st = try alloc.create(Self);
        st.* = .{
            .block = block,
            .capacity = cap,
            .connected = false,
            .file = undefined,
            .id = id,
            .mutable = false,
        };
        return st;
    }

    pub fn deinit(self: *Self) void {
        self.block.deinit();
        if (self.connected) {
            self.file.close();
        }
        self.* = undefined;
    }

    pub fn connect(self: *Self, file: std.fs.File) !void {
        self.file = file;
        self.connected = true;
        self.mutable = true;
    }

    pub fn read(self: Self, offset: u16, kv: *KV) !void {
        if (!self.connected) {
            return error.NotConnected;
        }
        if (self.block.count == 0) {
            return Error.NotFound;
        }
        try self.block.read(offset, kv);
    }

    pub fn write(self: *Self, value: KV) !usize {
        if (self.mutable and self.connected) {
            return try self.block.write(value);
        }
        return Error.WriteError;
    }

    pub fn flush(self: *Self) !void {
        if (self.mutable and self.connected) {
            self.block.flush(self.file) catch |err| {
                std.debug.print("sstable flush err: {s}\n", .{@errorName(err)});
                return err;
            };
        }
        return Error.WriteError;
    }
};

test "SSTable" {
    const FileUtils = @import("file.zig");

    const testing = std.testing;
    var alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "sstable.dat" });
    defer alloc.free(filename);
    const dopts = options.defaultOpts();
    const file = try FileUtils.openWithCapacity(filename, dopts.sst_capacity);

    // given
    var st = try SSTable.init(alloc, 1, dopts);
    defer alloc.destroy(st);
    defer st.deinit();
    try st.connect(file);

    // when
    var key: u64 = std.hash.Murmur2_64.hash("__key__");
    var expected: []const u8 = "__value__";
    var kv: KV = .{
        .hash = key,
        .key = "__key__",
        .value = expected,
    };

    var offset = try st.write(kv);

    var actual: KV = .{};
    try st.read(offset, &actual);

    // then
    try testing.expect(std.mem.eql(u8, expected, actual.value));

    // when
    key = std.hash.Murmur2_64.hash("__key_a__");
    expected = "__value_a__";
    kv.hash = key;
    kv.key = "__key_a__";
    kv.value = "__value_a__";

    offset = try st.write(kv);
    try st.read(offset, actual);

    // then
    try testing.expect(std.mem.eql(u8, expected, actual.value));
}
