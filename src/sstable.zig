const std = @import("std");

const blk = @import("block.zig");
const keyvalue = @import("kv.zig");
const mmap = @import("mmap.zig");
const options = @import("opts.zig");
const tm = @import("tablemap.zig");
const file_utils = @import("file.zig");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMap = std.StringHashMap;
const File = std.fs.File;

const Block = blk.Block;
const BlockMeta = blk.BlockMeta;
const KV = keyvalue.KV;
const MMap = mmap.AppendOnlyMMap;
const Opts = options.Opts;
const TableMap = tm.TableMap;

const Endian = std.builtin.Endian.little;
const PageSize = std.mem.page_size;

const assert = std.debug.assert;
const fixedBufferStream = std.io.fixedBufferStream;
const print = std.debug.print;

/// sstable[meta_data,block,block]
pub const SSTable = struct {
    alloc: Allocator,
    block: *Block,
    capacity: u64,
    connected: bool,
    data_dir: []const u8,
    file: File,
    id: u64,
    mutable: bool,
    stream: *MMap,

    const Self = @This();

    const Error = error{
        DuplicateError,
        NotConnected,
        NotFound,
        ReadError,
        WriteError,
    };

    const State = enum {
        immutable,
        mutable,
    };

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const block = try Block.init(alloc, opts.sst_capacity);
        const st = try alloc.create(Self);
        st.* = .{
            .alloc = alloc,
            .block = block,
            .capacity = opts.sst_capacity,
            .connected = false,
            .data_dir = opts.data_dir,
            .file = undefined,
            .id = id,
            .mutable = true,
            .stream = undefined,
        };
        return st;
    }

    pub fn deinit(self: *Self) void {
        self.block.deinit();
        self.alloc.destroy(self.block);
        if (self.connected) {
            self.stream.deinit();
            self.alloc.destroy(self.stream);
            self.file.close();
        }
        self.* = undefined;
    }

    pub fn open(self: *Self, file: File) !void {
        if (self.connected) {
            return;
        }

        const stat = try file.stat();
        const file_size = stat.size;

        var stream = try MMap.init(self.alloc, file_size);
        try stream.connect(file, 0);

        const reader = stream.buf.reader();

        const block_size = try reader.readInt(u64, Endian);
        if (block_size > 0) {
            const decoded_bytes = try self.block.decode(&stream.buf);
            assert(decoded_bytes == block_size);

            self.*.mutable = false;
        }

        self.*.connected = true;
        self.*.file = file;
        self.*.stream = stream;
    }

    pub fn read(self: Self, key: []const u8, kv: *KV) !void {
        // TODO: fix this implementation
        // idea 1: check block meta to see if the key is even in this block
        // idea 2: update API to accept an index instead of a key
        // idea 3: map keys and indices
        var stream = fixedBufferStream(self.block.offset_data.items);
        var stream_reader = stream.reader();

        while (stream.pos < stream.buffer.len) {
            const idx = try stream_reader.readInt(u64, Endian);

            const value = self.block.read(idx) catch |err| {
                print(
                    "sstable not able to read from block @ offset {d}: {s}\n",
                    .{ idx, @errorName(err) },
                );
                return err;
            };

            if (std.mem.eql(u8, value.key, key)) {
                kv.* = value;
                return;
            }
        }
    }

    pub fn write(self: *Self, value: KV) !usize {
        if (self.mutable) {
            const idx = self.block.write(value) catch |err| {
                print(
                    "sstable not able to write to block for key {s}: {s}\n",
                    .{ value.key, @errorName(err) },
                );
                return err;
            };
            return idx;
        }

        return Error.WriteError;
    }

    pub fn freeze(self: *Self) !void {
        if (self.connected and self.mutable) {
            try self.block.freeze();
        }
    }

    pub fn flush(self: *Self) !void {
        if (!self.connected and self.mutable) {
            try self.block.freeze();

            const filename = try std.fmt.allocPrint(
                self.alloc,
                "{s}/{d}.dat",
                .{ self.data_dir, self.id },
            );
            defer self.alloc.free(filename);

            const out_file = try file_utils.openWithCapacity(filename, self.block.size());
            try self.open(out_file);

            _ = self.block.flush(&self.stream.buf) catch |err| {
                print(
                    "sstable not able to flush block with count {d}: {s}\n",
                    .{ self.block.count, @errorName(err) },
                );
                return err;
            };

            self.file.sync() catch |err| {
                const meta = try self.file.metadata();
                print(
                    "sstable fsync failed: {s} : meta: kind {} size {d} perms {}",
                    .{ @errorName(err), meta.kind(), meta.size(), meta.permissions() },
                );
            };

            self.mutable = false;

            return;
        }

        return Error.WriteError;
    }
};

test SSTable {
    const testing = std.testing;
    var alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    var dopts = options.defaultOpts();
    dopts.data_dir = pathname;

    // given
    var st = try SSTable.init(alloc, 1, dopts);
    defer alloc.destroy(st);
    defer st.deinit();

    // when
    const expected = "__value__";
    const kv = KV.init("__key__", expected);

    _ = try st.write(kv);
    _ = try st.flush();

    var actual: KV = undefined;
    try st.read(kv.key, &actual);

    // then
    try testing.expectEqualStrings(expected, actual.value);
}
