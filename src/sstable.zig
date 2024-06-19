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
    alloc: Allocator,
    capacity: usize,
    data: FixedBuffer([]align(std.mem.page_size) u8),
    offset: FixedBuffer([]align(std.mem.page_size) u8),
    count: u16,

    const Self = @This();

    const Error = error{
        WriteError,
    };

    pub fn fromBuffer(alloc: Allocator, buffer: *AppendOnlyMMap) !*Self {
        const buf = buffer.buf;
        const reader = buf.reader();
        const count = try reader.readInt(u64, std.builtin.Endian.little);
        const offset_start_idx = @sizeOf(u64);
        const offset_len = count * @sizeOf(u16);
        const data_start_idx = @sizeOf(u64) + offset_len;
        const data_len = buf.getEndPos() - data_start_idx;
        const block = try alloc.create(Self);
        block.* = .{
            .alloc = alloc,
            .capacity = std.mem.page_size,
            .data = std.io.fixedBufferStream(buf.buffer[data_start_idx..data_len]),
            .offset = std.io.fixedBufferStream(buf.buffer[offset_start_idx..offset_len]),
            .count = count,
        };
        return block;
    }

    pub fn init(alloc: Allocator) !*Self {
        const capacity = std.mem.page_size;
        const data = try alloc.alloc(u8, capacity);
        const offset = try alloc.alloc(u8, capacity);
        const block = try alloc.create(Self);
        block.* = .{
            .alloc = alloc,
            .capacity = capacity,
            .data = std.io.fixedBufferStream(@as([]align(std.mem.page_size) u8, @alignCast(data))),
            .offset = std.io.fixedBufferStream(@as([]align(std.mem.page_size) u8, @alignCast(offset))),
            .count = 0,
        };
        return block;
    }

    pub fn deinit(self: *Self) void {
        self.alloc.free(self.data.buffer);
        self.alloc.free(self.offset.buffer);
        self.* = undefined;
    }

    pub fn write(self: Self, kv: KV) !usize {
        const offset = self.data.getPos();
        const size = @sizeOf(u64) + @sizeOf(u16) + kv.key.len + @sizeOf(u16) + kv.value.len;
        const remaining = self.data.getEndPos() - offset;
        if (size > remaining) {
            return error.WriteError;
        }

        const data_writer = self.data.writer();
        data_writer.writeInt(u64, kv.hash, std.builtin.Endian.little);
        data_writer.writeInt(u16, kv.key.len, std.builtin.Endian.little);
        data_writer.write(kv.key);
        data_writer.writeInt(u16, kv.value.len, std.builtin.Endian.little);
        data_writer.write(kv.value);

        const offset_writer = self.offset.writer();
        offset_writer.writeInt(u16, offset, std.builtin.Endian.little);

        self.count += 1;
    }

    /// [count, offset1, offset2, kv[hash, keylen, key, valuelen, value], kv[hash, keylen, key, valuelen, value]]
    pub fn flush(self: Self, buf: *AppendOnlyMMap) !void {
        var count: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        std.mem.writeInt(std.math.ByteAlignedInt(u64), &count, self.count, std.builtin.Endian.little);
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
    alloc: Allocator,
    block: *Block,
    blocks: std.ArrayList(*Block),
    capacity: usize,
    connected: bool,
    data: *AppendOnlyMMap,
    file: std.fs.File,
    id: u64,
    index: *TableMap(IndexValue),
    mutable: bool,

    const Self = @This();

    /// [meta,data]
    /// [meta[len,count,version],block[count,offset1,offset2,kv1,kv2],block[count,offset1,offset2,kv1,kv2]]
    const TableMeta = struct {
        len: u64,
        count: usize,
        version: u16,
    };

    const Error = error{
        NotFound,
    } || AppendOnlyMMap.Error;

    const IndexValue = struct { idx: usize, key: u64, len: u64 };

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const cap = opts.sst_capacity / @sizeOf(SSTableRow);
        const index = try TableMap(IndexValue).init(alloc);
        const data = try AppendOnlyMMap.init(alloc, opts.sst_capacity);
        const block = try Block.init(alloc);
        const blocks = std.ArrayList(*Block).init(alloc);
        const st = try alloc.create(Self);
        st.* = .{
            .alloc = alloc,
            .block = block,
            .blocks = blocks,
            .capacity = cap,
            .connected = false,
            .data = data,
            .file = undefined,
            .id = id,
            .index = index,
            .mutable = false,
        };
        return st;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.alloc.destroy(self.data);
        self.block.deinit();
        self.alloc.destroy(self.block);
        self.index.deinit();
        self.alloc.destroy(self.index);
        if (self.connected) {
            self.file.close();
        }
        self.* = undefined;
    }

    pub fn connect(self: *Self, file: std.fs.File) !void {
        // try self.data.connect(file);
        try self.block.connect(file);
        self.file = file;
        self.connected = true;
        self.mutable = true;
    }

    pub fn read(self: *Self, key: u64) ![]const u8 {
        if (!self.connected) {
            return error.NotConnected;
        }
        const count = self.data.getCount();
        if (count == 0) {
            return Error.NotFound;
        }
        const data = try self.index.get(key);
        const value = try self.alloc.alloc(u8, data.len);
        _ = try self.data.read(data.idx, value);
        return value;
    }

    pub fn write(self: *Self, key: u64, value: []const u8) !void {
        if (self.mutable and self.connected) {
            const result = try self.data.write(value);
            try self.index.put(key, IndexValue{
                .idx = result.idx,
                .key = key,
                .len = result.len,
            });
            return;
        }
        return Error.WriteError;
    }

    pub fn xwrite(self: *Self, value: KV) !void {
        if (self.mutable and self.connected) {
            try self.block.write(value);
            if (self.block.count() >= self.capacity) {
                try self.block.flush(self.data);
                self.blocks.append(self.block);
                self.block = try Block.init(self.alloc);
            }
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

    try st.write(key, expected);

    const actual = try st.read(key);
    defer alloc.free(actual);

    // then
    try testing.expect(std.mem.eql(u8, expected, actual));

    // when
    key = std.hash.Murmur2_64.hash("__key_a__");
    expected = "__value_a__";

    try st.write(key, expected);
    const newactual = try st.read(key);
    defer alloc.free(newactual);

    // then
    try testing.expect(std.mem.eql(u8, expected, newactual));
}
