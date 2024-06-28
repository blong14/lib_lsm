const std = @import("std");

const file_utils = @import("file.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tbm = @import("tablemap.zig");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Opts = options.Opts;
const SSTable = sst.SSTable;
const TableMap = tbm.TableMap;

pub fn Memtable(comptime K: type, comptime V: type) type {
    return struct {
        alloc: ArenaAllocator,
        hash_map: *TableMap(V),
        id: u64,
        opts: Opts,
        mutable: bool,
        sstable: *SSTable,

        const Self = @This();

        const Error = error{
            Full,
        };

        pub fn init(alloc: Allocator, id: u64, opts: Opts) !*Self {
            var arena = ArenaAllocator.init(alloc);
            const map = try TableMap(V).init(alloc);

            const allocator = arena.allocator();
            const mtable = try allocator.create(Self);
            mtable.* = .{
                .alloc = arena,
                .hash_map = map,
                .id = id,
                .mutable = true,
                .opts = opts,
                .sstable = undefined,
            };
            return mtable;
        }

        pub fn deinit(self: *Self) void {
            self.hash_map.deinit();
            self.alloc.deinit();
            self.* = undefined;
        }

        pub fn getId(self: Self) u64 {
            return self.id;
        }

        pub fn put(self: *Self, key: K, value: *V) !void {
            if (!self.mutable) {
                return error.Full;
            }
            try self.hash_map.put(key, value.*);
        }

        pub fn get(self: Self, key: K) ?V {
            return self.hash_map.get(key) catch return null;
        }

        pub fn count(self: Self) usize {
            return self.hash_map.count();
        }

        pub fn scan(self: *Self, start: u64, end: u64, out: *std.ArrayList(V)) !void {
            var scnr = try self.hash_map.scanner(start, end);
            while (scnr.hasNext()) {
                const entry = try scnr.next();
                try out.append(entry.?);
            }
        }

        pub const Iterator = struct {
            hash_iter: TableMap(V).Iterator,
            k: u64,
            v: V,

            pub fn key(it: Iterator) u64 {
                return it.k;
            }

            pub fn value(it: Iterator) V {
                return it.v;
            }

            pub fn next(it: *Iterator) bool {
                if (it.hash_iter.next()) {
                    it.*.k = it.hash_iter.key();
                    it.*.v = it.hash_iter.value();
                    return true;
                }
                return false;
            }

            /// Reset the iterator to the initial index
            pub fn reset(it: *Iterator) void {
                it.hash_iter.reset();
            }
        };

        pub fn iterator(self: *Self) !*Iterator {
            const hiter = self.hash_map.iterator(0);
            const iter = try self.alloc.allocator().create(Iterator);
            iter.* = .{ .hash_iter = hiter, .k = 0, .v = undefined };
            return iter;
        }

        pub fn flush(self: *Self) !void {
            if (!self.mutable or self.count() == 0) {
                return;
            }

            std.debug.print("flushing memtable...\n", .{});

            // TODO: Use a fixed buffer allocator here
            const allocator = self.alloc.allocator();
            const filename = try std.fmt.allocPrint(allocator, "{s}/{d}.dat", .{ self.opts.data_dir, self.getId() });
            defer allocator.free(filename);

            const file = try file_utils.openWithCapacity(filename, self.opts.sst_capacity);
            var sstable = try SSTable.init(allocator, self.getId(), self.opts);
            try sstable.connect(file);

            var iter = try self.iterator();
            while (iter.next()) {
                _ = sstable.write(iter.value()) catch {
                    std.debug.print("sstable write error\n", .{});
                    // return err;
                };
            }

            self.sstable = sstable;
            self.mutable = false;
        }
    };
}

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var mtable = try Memtable(u64, []const u8).init(alloc, 0, options.defaultOpts());
    defer alloc.destroy(mtable);
    defer mtable.deinit();

    // when
    try mtable.put(1, "__value__");
    const actual = mtable.get(1);

    // then
    try testing.expectEqualStrings("__value__", actual.?);

    // when
    var next_actual: []const u8 = undefined;
    var iter = try mtable.iterator();
    defer alloc.destroy(iter);
    if (iter.next()) {
        next_actual = iter.value();
    }

    // then
    try testing.expectEqualStrings("__value__", next_actual);
}
