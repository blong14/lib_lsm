const std = @import("std");

const mem = std.mem;

const blk = @import("block.zig");
const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const mt = @import("memtable.zig");
const mmap = @import("mmap.zig");
const options = @import("opts.zig");
const file_utils = @import("file.zig");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const File = std.fs.File;

const Block = blk.Block;
const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const Memtable = mt.Memtable;
const MergeIterator = iter.MergeIterator;
const MMap = mmap.AppendOnlyMMap;
const Opts = options.Opts;

const Endian = std.builtin.Endian.little;
const PageSize = std.mem.page_size;

const assert = std.debug.assert;
const bufPrint = std.fmt.bufPrint;
const fixedBufferStream = std.io.fixedBufferStream;
const parseInt = std.fmt.parseInt;
const print = std.debug.print;
const readInt = std.mem.readInt;
const startsWith = std.mem.startsWith;

pub const TableManager = struct {
    levels: ArrayList(ArrayList(*SSTable)),
    opts: Opts,
    num_levels: usize,

    const Level = struct {
        pub const Error = error{InvalidLevel};

        pub fn fromString(level_name: []const u8) !usize {
            // Parse level-N format to get the level number
            if (level_name.len < 7 or !startsWith(u8, level_name, "level-")) {
                return Error.InvalidLevel;
            }

            const level_num = parseInt(usize, level_name[6..], 10) catch {
                return Error.InvalidLevel;
            };

            return level_num;
        }

        pub fn toString(level: usize, buf: []u8) ![]const u8 {
            return bufPrint(buf, "level-{d}", .{level});
        }
    };

    const Self = @This();

    pub fn init(alloc: Allocator, opts: Opts) !Self {
        const default_levels = 3;
        const num_levels = opts.num_levels orelse default_levels;

        var levels = ArrayList(ArrayList(*SSTable)).init(alloc);

        var i: usize = 0;
        while (i < num_levels) : (i += 1) {
            try levels.append(ArrayList(*SSTable).init(alloc));
        }

        return .{
            .levels = levels,
            .opts = opts,
            .num_levels = num_levels,
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        for (self.levels.items) |*level| {
            for (level.items) |sstable| {
                sstable.deinit();
                alloc.destroy(sstable);
            }
            level.deinit();
        }

        self.levels.deinit();
        self.* = undefined;
    }

    pub fn open(self: *Self, alloc: Allocator) !void {
        var data_dir = try std.fs.openDirAbsolute(self.opts.data_dir, .{ .iterate = true });
        defer data_dir.close();

        var dir_iter = try data_dir.walk(alloc);
        defer dir_iter.deinit();

        while (try dir_iter.next()) |f| {
            const nxt_file = try std.fmt.allocPrint(
                alloc,
                "{s}/{s}",
                .{ self.opts.data_dir, f.basename },
            );
            errdefer alloc.free(nxt_file);

            const data_file = file_utils.open(nxt_file) catch |err| switch (err) {
                error.IsDir => {
                    alloc.free(nxt_file);
                    continue;
                },
                else => return err,
            };

            var filename_parts = mem.split(u8, f.basename, "_");
            const level_name = filename_parts.first();
            const sst_id = filename_parts.next().?;

            var sstable = try SSTable.init(alloc, sst_id, self.opts);
            try sstable.open(data_file);

            const level = try Level.fromString(level_name);
            try self.add(sstable, level);

            alloc.free(nxt_file);
        }
    }

    pub fn compact(self: *Self, level: usize) !void {
        _ = self;
        std.debug.print("compacting level {d}...\n", .{level});
    }

    pub fn add(self: *Self, st: *SSTable, level: usize) !void {
        if (level >= self.num_levels) {
            return error.InvalidLevel;
        }

        try self.levels.items[level].append(st);
    }

    pub fn get(self: Self, level: usize) []const *SSTable {
        if (level >= self.num_levels) {
            return &[_]*SSTable{};
        }

        return self.levels.items[level].items;
    }

    const LevelIterator = struct {
        alloc: Allocator,
        iter: Iterator(KV),
        merger: *MergeIterator(KV, keyvalue.compare),

        pub fn deinit(ctx: *anyopaque) void {
            const self: *LevelIterator = @ptrCast(@alignCast(ctx));
            self.iter.deinit();
            self.alloc.destroy(self.merger);
            self.alloc.destroy(self);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const self: *LevelIterator = @ptrCast(@alignCast(ctx));
            return self.iter.next();
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        var merger = try alloc.create(MergeIterator(KV, keyvalue.compare));
        merger.* = try MergeIterator(KV, keyvalue.compare).init(alloc);

        var level_idx: usize = 0;
        while (level_idx < self.num_levels) : (level_idx += 1) {
            for (self.get(level_idx)) |sstable| {
                const siter = try sstable.iterator(alloc);
                try merger.add(siter);
            }
        }

        const it = try alloc.create(LevelIterator);
        it.* = .{ .alloc = alloc, .merger = merger, .iter = merger.iterator() };

        return Iterator(KV).init(it, LevelIterator.next, LevelIterator.deinit);
    }

    pub fn flush(self: *Self, alloc: Allocator, mtable: *Memtable) !void {
        var sstable = try SSTable.init(alloc, mtable.getId(), self.opts);
        errdefer alloc.destroy(sstable);
        errdefer sstable.deinit();

        mtable.freeze();

        var it = try mtable.iterator(alloc);
        defer it.deinit();

        while (it.next()) |nxt| {
            _ = sstable.write(nxt) catch |err| switch (err) {
                error.DuplicateError => continue,
                else => {
                    print(
                        "memtable not able to write to sstable for key {s}: {s}\n",
                        .{ nxt.key, @errorName(err) },
                    );
                    return err;
                },
            };
        }

        sstable.flush() catch |err| @panic(@errorName(err));

        mtable.isFlushed.store(true, .release);

        try self.add(sstable, 0); // Add to level 0
    }
};

/// sstable[meta_data,block,block]
pub const SSTable = struct {
    alloc: Allocator,
    block: *Block,
    capacity: u64,
    connected: bool,
    data_dir: []const u8,
    file: File,
    id: []const u8,
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

    pub fn init(alloc: Allocator, id: []const u8, opts: Opts) !*Self {
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
            assert(decoded_bytes > 0);

            self.*.mutable = false;
        }

        stream.buf.reset();

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

    const SSTableIterator = struct {
        alloc: Allocator,
        block: *Block = undefined,
        nxt: usize = 0,

        pub fn deinit(ctx: *anyopaque) void {
            const self: *SSTableIterator = @ptrCast(@alignCast(ctx));
            self.alloc.destroy(self);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const self: *SSTableIterator = @ptrCast(@alignCast(ctx));

            const offset_sz = @sizeOf(u64);

            var kv: ?KV = null;
            if (self.nxt + offset_sz <= self.block.offset_data.items.len) {
                var nxt_offset_byts: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
                @memcpy(&nxt_offset_byts, self.block.offset_data.items[self.nxt .. self.nxt + offset_sz]);

                const nxt_offset = readInt(u64, &nxt_offset_byts, Endian);

                kv = self.block.read(nxt_offset) catch |err| {
                    print("sstable iter error {s}\n", .{@errorName(err)});
                    return null;
                };

                self.*.nxt += offset_sz;
            }

            return kv;
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        const it = try alloc.create(SSTableIterator);
        it.* = .{ .alloc = alloc, .block = self.block };
        return Iterator(KV).init(it, SSTableIterator.next, SSTableIterator.deinit);
    }

    pub fn flush(self: *Self) !void {
        if (!self.connected and self.mutable) {
            try self.block.freeze();

            const sz = self.block.size();

            const filename = try std.fmt.allocPrint(
                self.alloc,
                "{s}/level-0_{s}_{d}.dat",
                .{ self.data_dir, self.id, sz },
            );
            defer self.alloc.free(filename);

            const out_file = try file_utils.openAndTruncate(filename, sz);
            try self.open(out_file);

            _ = self.block.flush(&self.stream.buf) catch |err| {
                print(
                    "sstable not able to flush {s} block with count {d}: {s}\n",
                    .{ filename, self.block.count, @errorName(err) },
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
    var st = try SSTable.init(alloc, "1", dopts);
    defer alloc.destroy(st);
    defer st.deinit();

    // when
    const expected = "__value__";
    const kv = KV.init("__key__", expected);

    _ = try st.write(kv);

    var actual: KV = undefined;
    try st.read(kv.key, &actual);

    // then
    try testing.expectEqualStrings(expected, actual.value);

    // when
    const akv = KV.init("__another_key__", "__another_value__");

    _ = try st.write(akv);

    var siter = try st.iterator(alloc);
    defer siter.deinit();

    _ = siter.next();

    const nxt_actual = siter.next();

    try testing.expectEqualStrings(akv.value, nxt_actual.?.value);

    _ = try st.flush();
}
