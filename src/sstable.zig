const std = @import("std");

const KV = @import("kv.zig").KV;
const options = @import("opts.zig");

const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const FixedBuffer = std.io.FixedBufferStream;
const Opts = options.Opts;

pub const Block = struct {
    alloc: Allocator,
    data: ArrayList(u8),
    offset: ArrayList(u8),
    count: u16,
    errs: u16,

    const Self = @This();

    const Error = error{
        BlockReadError,
        BlockWriteError,
    };

    pub fn init(allocator: Allocator, opts: Opts) !*Self {
        const data = try ArrayList(u8).initCapacity(allocator, opts.sst_capacity);
        const offset = try ArrayList(u8).initCapacity(allocator, opts.sst_capacity);

        const block = try allocator.create(Self);
        block.* = .{
            .alloc = allocator,
            .data = data,
            .offset = offset,
            .count = 0,
            .errs = 0,
        };
        return block;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.offset.deinit();
        self.* = undefined;
    }

    pub fn read(self: *Self, offset: usize, kv: *KV) !void {
        try kv.decode(self.data.items[offset..]);
    }

    pub fn write(self: *Self, kv: *const KV) !usize {
        // [1 1 1 1 _ _ _ _]
        const offset = self.data.items.len;
        const offset_writer = self.offset.writer();
        try offset_writer.writeInt(usize, offset, std.builtin.Endian.little);

        var buf: [std.mem.page_size]u8 = undefined;
        const encoded_kv = try kv.encode(&buf);

        const data_writer = self.data.writer();
        const byte_count = try data_writer.write(encoded_kv);
        assert(byte_count == encoded_kv.len);

        self.count += 1;
        return offset;
    }

    /// [count, offset1, offset2, kv[hash, keylen, key, valuelen, value], kv[hash, keylen, key, valuelen, value]]
    pub fn flush(self: Self, outFile: std.fs.File) !void {
        var count: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        std.mem.writeInt(std.math.ByteAlignedInt(u64), &count, self.count, std.builtin.Endian.little);

        const buf = outFile.writer();
        _ = try buf.write(&count);
        _ = try buf.write(self.offset.items);
        _ = try buf.write(self.data.items);
    }
};

pub const SSTable = struct {
    alloc: Allocator,
    block: *Block,
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

    const Error = error{
        NotFound,
        ReadError,
        WriteError,
    };

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const block = try Block.init(alloc, opts);
        const st = try alloc.create(Self);
        st.* = .{
            .alloc = alloc,
            .block = block,
            .connected = false,
            .file = undefined,
            .id = id,
            .mutable = false,
        };
        return st;
    }

    pub fn deinit(self: *Self) void {
        self.block.deinit();
        self.alloc.destroy(self.block);
        if (self.connected) {
            self.file.close();
        }
        self.* = undefined;
    }

    pub fn connect(self: *Self, file: std.fs.File) !void {
        self.file = file;
        self.*.connected = true;
        self.*.mutable = true;
    }

    pub fn read(self: Self, offset: usize, kv: *KV) !void {
        if (!self.connected) {
            return error.NotConnected;
        }
        if (self.block.count == 0) {
            return Error.NotFound;
        }
        self.block.read(offset, kv) catch |err| {
            std.debug.print(
                "sstable not able to read from block @ offset {d}: {s}\n",
                .{ offset, @errorName(err) },
            );
            return err;
        };
    }

    pub fn write(self: *Self, value: *const KV) !usize {
        if (self.mutable and self.connected) {
            return self.block.write(value) catch |err| {
                std.debug.print(
                    "sstable not able to write to block for key {s}: {s}\n",
                    .{ value.key, @errorName(err) },
                );
                return err;
            };
        }
        return Error.WriteError;
    }

    pub fn flush(self: *Self) !void {
        if (self.mutable and self.connected) {
            self.block.flush(self.file) catch |err| {
                std.debug.print(
                    "sstable not able to flush block: {s}\n",
                    .{@errorName(err)},
                );
                return err;
            };
            self.mutable = false;
            self.file.sync() catch |err| {
                const meta = try self.file.metadata();
                std.debug.print(
                    "sstable fsync faild: {s} : meta: kind {} size {d} perms {}",
                    .{ @errorName(err), meta.kind(), meta.size(), meta.permissions() },
                );
            };
            return;
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
    const expected = "__value__";
    const kv = try KV.init(alloc, "__key__", expected);
    defer alloc.destroy(kv);

    const offset = try st.write(kv);
    try st.flush();

    const actual = try KV.init(alloc, undefined, undefined);
    defer alloc.destroy(actual);

    try st.read(offset, actual);

    // then
    try testing.expect(std.mem.eql(u8, expected, actual.value));
}
