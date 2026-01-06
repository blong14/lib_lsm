/// SSTable - Sorted String Table implementation
///
/// Structure: [metadata][block_data][block_data]...
const std = @import("std");

const iter = @import("iterator.zig");
const options = @import("opts.zig");

const Allocator = std.mem.Allocator;
const File = std.fs.File;

const Block = @import("Block.zig");
const Iterator = iter.Iterator;
const KV = @import("KV.zig");
const Opts = options.Opts;

const MAGIC: u32 = 0xdeadbeef;
const VERSION: u32 = 1;
const Endian = std.builtin.Endian.little;

const Header = packed struct {
    magic: u32,
    version: u32,
    id_len: u64,
};

alloc: Allocator,
file: File,
block: Block,
stream: []align(std.heap.page_size_min) u8,
capacity: u64,
data_dir: []const u8,
id: []const u8,

pub const SSTable = @This();

fn init(alloc: Allocator, opts: Opts) !*SSTable {
    const st = try alloc.create(SSTable);
    st.* = .{
        .alloc = alloc,
        .capacity = opts.sst_capacity,
        .data_dir = opts.data_dir,
        .id = undefined,
        .block = undefined,
        .file = undefined,
        .stream = undefined,
    };
    return st;
}

pub fn createFile(alloc: Allocator, level: usize, opts: Opts) !*SSTable {
    const sstid = try std.fmt.allocPrint(
        alloc,
        "level-{d}_{d}",
        .{ level, std.time.milliTimestamp() },
    );
    errdefer alloc.free(sstid);

    const sst = try SSTable.init(alloc, opts);
    errdefer {
        sst.deinit();
        alloc.destroy(sst);
    }

    sst.*.id = sstid;

    var buffer: [256]u8 = undefined;
    const file_path = std.fmt.bufPrint(&buffer, "{s}/{s}.dat", .{
        opts.data_dir,
        sst.id,
    }) catch unreachable;

    const file = try std.fs.cwd().createFile(file_path, .{ .read = true, .truncate = false });
    try file.setEndPos(sst.capacity);

    const stat = try file.stat();
    const file_size = stat.size;

    const stream = try std.posix.mmap(
        null,
        file_size,
        std.posix.PROT.READ | std.posix.PROT.WRITE,
        .{ .TYPE = .SHARED, .ANONYMOUS = false },
        file.handle,
        0,
    );

    var pos: usize = 0;

    const header: Header = .{
        .magic = MAGIC,
        .version = VERSION,
        .id_len = sst.id.len,
    };
    @memcpy(stream[pos..@sizeOf(Header)], std.mem.asBytes(&header));

    pos += @sizeOf(Header);

    @memcpy(stream[pos..][0..sst.id.len], sst.id);

    pos += sst.id.len;

    sst.*.file = file;
    sst.*.stream = stream;
    sst.*.block = .init(
        stream[pos..],
        .{ .max_offset_bytes = @divTrunc(sst.capacity, 4) },
    );

    return sst;
}

pub fn openFile(alloc: Allocator, file: File, opts: Opts) !*SSTable {
    const stat = try file.stat();
    const file_size = stat.size;

    const stream = try std.posix.mmap(
        null,
        file_size,
        std.posix.PROT.READ | std.posix.PROT.WRITE,
        .{ .TYPE = .SHARED, .ANONYMOUS = false },
        file.handle,
        0,
    );

    var pos: usize = 0;

    var header: Header = undefined;
    @memcpy(std.mem.asBytes(&header), stream[pos..@sizeOf(Header)]);

    pos += @sizeOf(Header);

    const sst = try SSTable.init(alloc, opts);
    errdefer {
        sst.deinit();
        alloc.destroy(sst);
    }

    const id = try alloc.alloc(u8, header.id_len);
    errdefer alloc.free(id);

    @memcpy(id, stream[pos..][0..header.id_len]);
    sst.*.id = id;

    pos += header.id_len;

    const size = std.mem.readInt(u64, stream[pos..][0..@sizeOf(u64)], Endian);
    if (size == 0) return error.EmptyFile;

    const first_offset = pos + @sizeOf(u64);

    // find the first data offset
    const data_start = std.mem.readInt(u64, stream[first_offset..][0..@sizeOf(u64)], Endian);

    sst.*.file = file;
    sst.*.stream = stream;
    sst.*.block = .init(
        stream[pos..],
        .{ .max_offset_bytes = data_start },
    );

    return sst;
}

pub fn deinit(self: *SSTable) void {
    std.posix.munmap(self.stream);
    self.file.sync() catch {};
    self.file.close();
    self.alloc.free(self.id);
}

pub fn read(self: SSTable, key: []const u8) !?KV {
    // TODO: Add bloom filter check here first
    // if (!self.bloom_filter.mightContain(key)) {
    //     return null;
    // }

    var start: usize = 0;
    var end: usize = self.block.len();

    while (start < end) {
        const mid: usize = start + (end - start) / 2;

        const value = try self.block.read(mid);

        const cmp = std.mem.order(u8, key, value.key);
        if (cmp == .eq) {
            return value;
        } else if (cmp == .lt) {
            end = mid;
        } else {
            start = mid + 1;
        }
    }

    return null;
}

pub fn write(self: *SSTable, value: KV) !usize {
    return self.block.write(value) catch |err| switch (err) {
        error.NoSpaceLeft, error.NoOffsetSpaceLeft => {
            return error.BufferFull;
        },
        else => return err,
    };
}

const SSTableIterator = struct {
    alloc: Allocator,
    block: *Block,
    nxt: usize = 0,

    pub fn deinit(ctx: *anyopaque) void {
        const self: *SSTableIterator = @ptrCast(@alignCast(ctx));
        self.alloc.destroy(self);
    }

    pub fn next(ctx: *anyopaque) ?KV {
        const self: *SSTableIterator = @ptrCast(@alignCast(ctx));

        var kv: ?KV = null;
        if (self.nxt < self.block.len()) {
            kv = self.block.read(self.nxt) catch |err| {
                std.log.err("sstable iter error {s}", .{@errorName(err)});
                return null;
            };

            self.*.nxt += 1;
        }

        return kv;
    }
};

pub fn iterator(self: *SSTable, alloc: Allocator) !Iterator(KV) {
    const it = try alloc.create(SSTableIterator);
    it.* = .{ .alloc = alloc, .block = &self.block };
    return Iterator(KV).init(it, SSTableIterator.next, SSTableIterator.deinit);
}

test "SSTable basic operations" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const testDir = testing.tmpDir(.{});

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    var dopts = options.defaultOpts();
    dopts.data_dir = pathname;

    const st = try SSTable.createFile(alloc, 0, dopts);
    defer alloc.destroy(st);
    defer st.deinit();

    {
        // Test read/write
        const expected = "__value__";
        const kv = try KV.init(alloc, "__key__", expected);

        _ = try st.write(kv);

        const actual = try st.read(kv.key);

        try testing.expectEqualStrings(expected, actual.?.value);
    }

    {
        // Test multiple entries and iteration
        const akv = try KV.init(alloc, "__another_key__", "__another_value__");
        _ = try st.write(akv);

        var siter = try st.iterator(alloc);
        defer siter.deinit();

        _ = siter.next();
        const nxt_actual = siter.next();

        try testing.expectEqualStrings(akv.value, nxt_actual.?.value);
    }
}

test "SSTable persistence and error handling" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const testDir = testing.tmpDir(.{});

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    // defer testDir.dir.deleteTree(pathname) catch {};

    var dopts = options.defaultOpts();
    dopts.data_dir = pathname;

    // Create and populate an SSTable
    {
        const st = try SSTable.createFile(alloc, 0, dopts);
        defer {
            st.deinit();
            alloc.destroy(st);
        }

        _ = try st.write(try KV.init(alloc, "key1", "value1"));
        _ = try st.write(try KV.init(alloc, "key2", "value2"));

        try st.file.sync();
    }

    // Test error handling - key not found
    {
        // Open the directory to find the SSTable file
        var dir = try std.fs.openDirAbsolute(pathname, .{ .iterate = true });
        defer dir.close();

        var it = dir.iterate();
        const entry = (try it.next()) orelse return error.FileNotFound;

        const file = try dir.openFile(entry.name, .{ .mode = .read_write });

        var st = try SSTable.openFile(alloc, file, dopts);
        defer {
            st.deinit();
            alloc.destroy(st);
        }

        // Try to read a key that doesn't exist
        try testing.expectEqual(null, try st.read("nonexistent_key"));

        // Try to write to an immutable table
        try testing.expectError(error.WriteError, st.write(try KV.init(alloc, "new_key", "new_value")));
    }
}
