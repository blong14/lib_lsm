const std = @import("std");

const blk = @import("block.zig");
const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const options = @import("opts.zig");
const file_utils = @import("file.zig");

const Allocator = std.mem.Allocator;
const File = std.fs.File;
const AtomicU32 = std.atomic.Value(u32);

const Block = blk.Block;
const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const Opts = options.Opts;

const Endian = std.builtin.Endian.little;
const PageSize = std.mem.page_size;

/// SSTable - Sorted String Table implementation
///
/// Structure: [metadata][block_data][block_data]...
pub const SSTable = struct {
    alloc: Allocator,
    block: *Block,
    capacity: u64,
    connected: bool,
    data_dir: []const u8,
    file: File,
    id: []const u8,
    mutable: bool,
    stream: []align(PageSize) u8,
    ref_count: AtomicU32,

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

    pub fn init(alloc: Allocator, level: usize, opts: Opts) !*Self {
        const sstid = try std.fmt.allocPrint(
            alloc,
            "level-{d}_{d}",
            .{ level, std.time.milliTimestamp() },
        );
        errdefer alloc.free(sstid);

        const st = try alloc.create(Self);
        st.* = .{
            .alloc = alloc,
            .capacity = opts.sst_capacity,
            .data_dir = opts.data_dir,
            .id = sstid,
            .mutable = true,
            .connected = false,
            .block = undefined,
            .file = undefined,
            .stream = undefined,
            .ref_count = AtomicU32.init(1), // Initialize with 1 reference
        };

        return st;
    }

    pub fn deinit(self: *Self) void {
        if (self.connected) {
            self.block.deinit();
            self.alloc.destroy(self.block);

            std.posix.munmap(self.stream);

            self.file.close();
            self.connected = false;
        }

        if (self.id.len > 0) {
            self.alloc.free(self.id);
        }
    }

    pub fn retain(self: *Self) void {
        _ = self.ref_count.fetchAdd(1, .monotonic);
    }

    pub fn release(self: *Self) bool {
        const prev = self.ref_count.fetchSub(1, .monotonic);
        // Return true if this was the last reference
        return prev == 1;
    }

    pub fn getRefCount(self: *Self) u32 {
        return self.ref_count.load(.monotonic);
    }

    pub fn open(self: *Self, file: File) !void {
        if (self.connected) {
            return;
        }

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

        var fbs = std.io.fixedBufferStream(stream);

        var reader = fbs.reader();
        const first_key_len = try reader.readInt(u64, Endian);

        var blck: *Block = undefined;
        if (first_key_len > 0) {
            self.*.mutable = false;
            blck = try Block.initFromData(self.alloc, stream);
        } else {
            blck = try self.alloc.create(Block);
            blck.* = Block.init(self.alloc, stream);
        }

        self.*.connected = true;
        self.*.file = file;
        self.*.block = blck;
        self.*.stream = stream;
    }

    pub fn read(self: Self, key: []const u8) !?KV {
        // TODO: Add bloom filter check here first
        // if (!self.bloom_filter.mightContain(key)) {
        //     return null;
        // }

        var start: usize = 0;
        var end: usize = self.block.len();

        while (start < end) {
            const mid: usize = start + (end - start) / 2;

            const value = self.block.read(mid) catch |err| {
                std.log.err(
                    "sstable not able to read from block @ index {d}: {s}",
                    .{ mid, @errorName(err) },
                );
                return err;
            };

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

    pub fn write(self: *Self, value: KV) !usize {
        if (!self.mutable) return Error.WriteError;
        if (!self.connected) return Error.NotConnected;

        const idx = self.block.write(value) catch |err| {
            std.log.err(
                "sstable not able to write to block for key {s}: {s}",
                .{ value.key, @errorName(err) },
            );
            return err;
        };

        return idx;
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

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        const it = try alloc.create(SSTableIterator);
        it.* = .{ .alloc = alloc, .block = self.block };
        return Iterator(KV).init(it, SSTableIterator.next, SSTableIterator.deinit);
    }
};

fn setupSSTable(alloc: Allocator, dopts: Opts) !*SSTable {
    var st = try SSTable.init(alloc, 0, dopts);

    const filename = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}.dat",
        .{ dopts.data_dir, st.id },
    );
    defer alloc.free(filename);

    const file = try file_utils.openAndTruncate(filename, st.capacity);
    try st.open(file);

    return st;
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

    var st = try setupSSTable(alloc, dopts);
    defer alloc.destroy(st);
    defer st.deinit();

    {
        // Test read/write
        const expected = "__value__";
        const kv = try KV.initOwned(alloc, "__key__", expected);

        _ = try st.write(kv);

        const actual = try st.read(kv.key);

        try testing.expectEqualStrings(expected, actual.?.value);
    }

    {
        // Test multiple entries and iteration
        const akv = try KV.initOwned(alloc, "__another_key__", "__another_value__");
        _ = try st.write(akv);

        var siter = try st.iterator(alloc);
        defer siter.deinit();

        _ = siter.next();
        const nxt_actual = siter.next();

        try testing.expectEqualStrings(akv.value, nxt_actual.?.value);
    }

    {
        // Test reference counting
        try testing.expectEqual(@as(u32, 1), st.getRefCount());

        st.retain();

        try testing.expectEqual(@as(u32, 2), st.getRefCount());

        const wasLast = st.release();

        try testing.expectEqual(false, wasLast);
        try testing.expectEqual(@as(u32, 1), st.getRefCount());
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
    defer testDir.dir.deleteTree(pathname) catch {};

    var dopts = options.defaultOpts();
    dopts.data_dir = pathname;

    // Create and populate an SSTable
    {
        var st = try setupSSTable(alloc, dopts);
        defer alloc.destroy(st);
        defer st.deinit();

        _ = try st.write(try KV.initOwned(alloc, "key1", "value1"));
        _ = try st.write(try KV.initOwned(alloc, "key2", "value2"));

        try st.block.flush();
        try st.file.sync();
    }

    // Test error handling - key not found
    {
        // Open the directory to find the SSTable file
        var dir = try std.fs.openDirAbsolute(pathname, .{ .iterate = true });
        defer dir.close();

        var it = dir.iterate();
        const entry = (try it.next()) orelse return error.FileNotFound;

        // Open with read-write permissions
        const file = try dir.openFile(entry.name, .{ .mode = .read_write });

        var st = try SSTable.init(alloc, 0, dopts);
        defer alloc.destroy(st);
        defer st.deinit();

        try st.open(file);

        // Try to read a key that doesn't exist
        try testing.expectEqual(null, try st.read("nonexistent_key"));

        // Try to write to an immutable table
        try testing.expectError(error.WriteError, st.write(try KV.initOwned(alloc, "new_key", "new_value")));
    }
}
