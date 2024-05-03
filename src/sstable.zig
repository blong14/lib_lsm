const std = @import("std");

const File = @import("file.zig");
const MMap = @import("mmap.zig").MMap;
const TableMap = @import("tablemap.zig").TableMap;
const options = @import("opts.zig");

const Allocator = std.mem.Allocator;
const Opts = options.Opts;

pub const SSTableRow = extern struct {
    key: u64,
    value: [*c]const u8,
};

// pub const XSSTableRow = struct {
//     key: usize,
//     size: usize,
// };

pub const SSTable = struct {
    alloc: Allocator,
    capacity: usize,
    data: *MMap(SSTableRow),
    file: std.fs.File,

    const Self = @This();

    const Error = error{
        NotFound,
    } || MMap(SSTableRow).Error;

    pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
        const pathname = opts.data_dir;
        const filename = try std.fmt.allocPrint(alloc, "{s}/{d}.dat", .{ pathname, id });
        defer alloc.free(filename);

        const capacity = opts.sst_capacity;
        const file = try File.openWithCapacity(filename, capacity);
        const data = try MMap(SSTableRow).init(alloc, capacity);
        try data.connect(file);
        const cap = capacity / @sizeOf(SSTableRow);
        const st = try alloc.create(Self);
        st.* = .{ .alloc = alloc, .capacity = cap, .data = data, .file = file };
        return st;
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
        self.alloc.destroy(self.data);
        self.file.close();
        self.* = undefined;
    }

    fn findIndex(self: *Self, key: u64, low: usize, high: usize) !usize {
        if (high < low) {
            return high + 1;
        }
        const mid = low + ((high - low) / 2);
        const entry = try self.data.read(mid);
        if (key < entry.key) {
            if (mid == 0) {
                return mid;
            }
            return self.findIndex(key, low, mid - 1);
        } else if (key == entry.key) {
            return mid;
        } else {
            return self.findIndex(key, mid + 1, high);
        }
    }

    fn equalto(self: *Self, key: u64, idx: usize) bool {
        const entry = self.data.read(idx) catch |err| {
            std.debug.print("equalto {s}\n", .{@errorName(err)});
            return false;
        };
        return key == entry.key;
    }

    fn greaterthan(self: *Self, key: u64, idx: usize) bool {
        const entry = self.data.read(idx) catch |err| {
            std.debug.print("greaterthan {s}\n", .{@errorName(err)});
            return false;
        };
        return key > entry.key;
    }

    pub fn read(self: *Self, key: u64) anyerror![]const u8 {
        const count = self.data.count;
        if (count == 0) {
            return Error.NotFound;
        }
        const idx = try self.findIndex(key, 0, count - 1);
        if ((idx == -1) or (idx == count)) {
            return Error.NotFound;
        }
        const row = try self.data.read(idx);
        return std.mem.span(row.value);
    }

    pub fn write(self: *Self, key: u64, value: []const u8) !void {
        const count: usize = self.data.getCount();
        if ((count == 0) or (self.greaterthan(key, count - 1))) {
            return try self.data.append(SSTableRow{ .key = key, .value = value.ptr });
        }
        const idx = try self.findIndex(key, 0, count - 1);
        try self.data.insert(idx, SSTableRow{ .key = key, .value = value.ptr });
    }

    pub fn append(self: *Self, key: u64, value: []const u8) !void {
        return try self.data.append(SSTableRow{ .key = key, .value = value.ptr });
    }
};

pub const KV = struct {
    idx: u64,
    key: []const u8,
    value: []const u8,
};

// pub const XSSTable = struct {
//     alloc: Allocator,
//     capacity: usize,
//     data: *XMMap,
//     file: std.fs.File,
//     index: TableMap(XSSTableRow),
//
//     const Self = @This();
//
//     const Error = error{
//         NotFound,
//     } || MMap(SSTableRow).Error;
//
//     pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
//         const pathname = opts.data_dir;
//         const filename = try std.fmt.allocPrint(alloc, "{s}/{d}.dat", .{ pathname, id });
//         defer alloc.free(filename);
//
//         const capacity = opts.sst_capacity;
//         const file = try File.openWithCapacity(filename, capacity);
//         const data = try XMMap.init(alloc, file, capacity);
//         const idx = try TableMap(XSSTableRow).init(alloc);
//         const cap = capacity / @sizeOf(SSTableRow);
//         const st = try alloc.create(Self);
//         st.* = .{ .alloc = alloc, .capacity = cap, .data = data, .file = file, .index = idx };
//         return st;
//     }
//
//     pub fn deinit(self: *Self) void {
//         self.data.deinit();
//         self.alloc.destroy(self.data);
//         self.file.close();
//         self.* = undefined;
//     }
//
//     fn findIndex(self: *Self, key: u64, low: usize, high: usize) !usize {
//         if (high < low) {
//             return high + 1;
//         }
//         const mid = low + ((high - low) / 2);
//         const entry = try self.data.read(mid);
//         if (key < entry.key) {
//             if (mid == 0) {
//                 return mid;
//             }
//             return self.findIndex(key, low, mid - 1);
//         } else if (key == entry.key) {
//             return mid;
//         } else {
//             return self.findIndex(key, mid + 1, high);
//         }
//     }
//
//     pub fn read(self: *Self, key: u64) !KV {
//         const count = self.data.count;
//         if (count == 0) {
//             return Error.NotFound;
//         }
//
//         const idx = try self.findIndex(key, 0, count - 1);
//         if ((idx == -1) or (idx == count)) {
//             return Error.NotFound;
//         }
//
//         const value = self.index.get(idx);
//         const buf = try self.alloc.alloc(u8, value.size);
//         try self.data.read(value.idx, buf);
//
//         const base64 = std.base64.standard;
//         const out = try self.alloc.alloc(u8, base64.Decoder.calcSize(value.size));
//         try base64.Decoder.decode(out, buf);
//
//         return .{
//             .idx = buf[1],
//             .key = buf[2..],
//             .value = buf[3..],
//         };
//     }
//
//     pub fn write(self: *Self, data: KV) !void {
//         const size: usize = data.key.len + data.value.len + 2;
//         const buf = try self.alloc.alloc(u8, size);
//         var stream = std.io.fixedBufferStream(buf);
//
//         const writer = stream.writer();
//         writer.writeInt(usize, size, std.builtin.Endian.little);
//         writer.writeInt(u64, data.idx, std.builtin.Endian.little);
//         writer.write(data.key);
//         writer.writeByte("::");
//         writer.write(data.value);
//         writer.writeByte("\n");
//
//         const base64 = std.base64.standard;
//         var out = try self.alloc.alloc(u8, base64.Encoder.calcSize(stream.buffer.len));
//         out = base64.Encoder.encode(out, stream.buffer);
//         try self.data.write(out);
//     }
// };

test "SSTable" {
    const testing = std.testing;
    var alloc = testing.allocator;

    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    // given
    const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pathname, "sstable.dat" });
    defer alloc.free(filename);

    const dopts = options.defaultOpts();
    var st = try SSTable.init(alloc, 1, dopts);
    defer alloc.destroy(st);
    defer st.deinit();

    // when
    var key: u64 = std.hash.Murmur2_64.hash("__key__");
    var expected: []const u8 = "__value__";
    try st.write(key, expected);

    // then
    var actual = try st.read(key);
    try testing.expect(std.mem.eql(u8, expected, actual));

    // when
    key = std.hash.Murmur2_64.hash("__key_a__");
    expected = "__value_a__";
    try st.write(key, expected);

    // then
    actual = try st.read(key);
    try testing.expect(std.mem.eql(u8, expected, actual));
}
