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

pub const SSTableStore = struct {
    levels: ArrayList(ArrayList(*SSTable)),
    opts: Opts,
    num_levels: usize,
    compaction_strategy: CompactionStrategy,
    stats: LevelStats,

    const Level = struct {
        pub const Error = error{InvalidLevel};

        pub fn fromString(level_name: []const u8) !usize {
            // Parse `level-n` format to get the level number
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
        const num_levels = opts.num_levels orelse 3;

        var levels = ArrayList(ArrayList(*SSTable)).init(alloc);

        var i: usize = 0;
        while (i < num_levels) : (i += 1) {
            try levels.append(ArrayList(*SSTable).init(alloc));
        }

        var strategy: CompactionStrategy = undefined;
        if (opts.compaction_strategy) |strat| {
            switch (strat) {
                .simple => {
                    const simple_strategy = try alloc.create(SimpleCompactionStrategy);
                    simple_strategy.* = SimpleCompactionStrategy.init(alloc);
                    strategy = CompactionStrategy.init(
                        simple_strategy,
                        SimpleCompactionStrategy.compact,
                        SimpleCompactionStrategy.deinit,
                    );
                },
                .tiered => {
                    const tiered_strategy = try alloc.create(TieredCompactionStrategy);
                    tiered_strategy.* = TieredCompactionStrategy.init(alloc);
                    strategy = CompactionStrategy.init(
                        tiered_strategy,
                        TieredCompactionStrategy.compact,
                        TieredCompactionStrategy.deinit,
                    );
                },
            }
        } else {
            const noop_strategy = try alloc.create(NoopCompactionStrategy);
            noop_strategy.* = NoopCompactionStrategy.init(alloc);
            strategy = CompactionStrategy.init(
                noop_strategy,
                NoopCompactionStrategy.compact,
                NoopCompactionStrategy.deinit,
            );
        }

        return .{
            .levels = levels,
            .opts = opts,
            .num_levels = num_levels,
            .compaction_strategy = strategy,
            .stats = LevelStats.init(alloc, num_levels),
        };
    }

    pub fn setCompactionStrategy(self: *Self, alloc: Allocator, strategy: CompactionStrategy) void {
        self.compaction_strategy.deinit(alloc);
        self.compaction_strategy = strategy;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        for (self.levels.items) |*level| {
            for (level.items) |sstable| {
                sstable.deinit();
                alloc.destroy(sstable);
            }
            level.deinit();
        }

        self.compaction_strategy.deinit(alloc);
        self.levels.deinit();
        self.stats.printStats();
        self.stats.deinit();
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
            const level = try Level.fromString(level_name);

            var sstable = try SSTable.init(alloc, level, self.opts);
            try sstable.open(data_file);

            try self.add(sstable, level);

            alloc.free(nxt_file);
        }
    }

    // Statistics tracking for each level to support compaction decisions
    pub const LevelStats = struct {
        alloc: Allocator,
        // Core metrics
        bytes_written: []std.atomic.Value(u64),
        bytes_read: []std.atomic.Value(u64),
        files_count: []std.atomic.Value(u32),
        total_keys: []std.atomic.Value(u64),

        // Time-based metrics
        last_compaction: []std.atomic.Value(i64),
        compaction_count: []std.atomic.Value(u32),

        // Performance metrics
        avg_read_latency_ns: []std.atomic.Value(u64),
        avg_write_latency_ns: []std.atomic.Value(u64),

        // Custom metrics storage for strategy-specific needs
        custom_metrics: std.StringHashMap(u64),
        mutex: std.Thread.Mutex,

        pub fn init(alloc: Allocator, num_levels: usize) LevelStats {
            var bytes_written = alloc.alloc(std.atomic.Value(u64), num_levels) catch @panic("OOM");
            var bytes_read = alloc.alloc(std.atomic.Value(u64), num_levels) catch @panic("OOM");
            var files_count = alloc.alloc(std.atomic.Value(u32), num_levels) catch @panic("OOM");
            var total_keys = alloc.alloc(std.atomic.Value(u64), num_levels) catch @panic("OOM");
            var last_compaction = alloc.alloc(std.atomic.Value(i64), num_levels) catch @panic("OOM");
            var compaction_count = alloc.alloc(std.atomic.Value(u32), num_levels) catch @panic("OOM");
            var avg_read_latency_ns = alloc.alloc(std.atomic.Value(u64), num_levels) catch @panic("OOM");
            var avg_write_latency_ns = alloc.alloc(std.atomic.Value(u64), num_levels) catch @panic("OOM");

            for (0..num_levels) |i| {
                bytes_written[i] = std.atomic.Value(u64).init(0);
                bytes_read[i] = std.atomic.Value(u64).init(0);
                files_count[i] = std.atomic.Value(u32).init(0);
                total_keys[i] = std.atomic.Value(u64).init(0);
                last_compaction[i] = std.atomic.Value(i64).init(0);
                compaction_count[i] = std.atomic.Value(u32).init(0);
                avg_read_latency_ns[i] = std.atomic.Value(u64).init(0);
                avg_write_latency_ns[i] = std.atomic.Value(u64).init(0);
            }

            return .{
                .alloc = alloc,
                .bytes_written = bytes_written,
                .bytes_read = bytes_read,
                .files_count = files_count,
                .total_keys = total_keys,
                .last_compaction = last_compaction,
                .compaction_count = compaction_count,
                .avg_read_latency_ns = avg_read_latency_ns,
                .avg_write_latency_ns = avg_write_latency_ns,
                .custom_metrics = std.StringHashMap(u64).init(alloc),
                .mutex = std.Thread.Mutex{},
            };
        }

        pub fn deinit(self: *LevelStats) void {
            self.alloc.free(self.bytes_written);
            self.alloc.free(self.bytes_read);
            self.alloc.free(self.files_count);
            self.alloc.free(self.total_keys);
            self.alloc.free(self.last_compaction);
            self.alloc.free(self.compaction_count);
            self.alloc.free(self.avg_read_latency_ns);
            self.alloc.free(self.avg_write_latency_ns);
            self.custom_metrics.deinit();
        }

        pub fn recordBytesWritten(self: *LevelStats, level: usize, bytes: u64) void {
            if (level >= self.bytes_written.len) return;
            _ = self.bytes_written[level].fetchAdd(bytes, .monotonic);
        }

        pub fn recordBytesRead(self: *LevelStats, level: usize, bytes: u64) void {
            if (level >= self.bytes_read.len) return;
            _ = self.bytes_read[level].fetchAdd(bytes, .monotonic);
        }

        pub fn recordKeysWritten(self: *LevelStats, level: usize, count: u64) void {
            if (level >= self.total_keys.len) return;
            _ = self.total_keys[level].fetchAdd(count, .monotonic);
        }

        pub fn recordCompaction(self: *LevelStats, level: usize) void {
            if (level >= self.last_compaction.len) return;
            self.last_compaction[level].store(std.time.timestamp(), .release);
            _ = self.compaction_count[level].fetchAdd(1, .monotonic);
        }

        pub fn updateReadLatency(self: *LevelStats, level: usize, latency_ns: u64) void {
            if (level >= self.avg_read_latency_ns.len) return;
            // Simple exponential moving average
            const old_avg = self.avg_read_latency_ns[level].load(.acquire);
            const new_avg = if (old_avg == 0) latency_ns else (old_avg * 3 + latency_ns) / 4;
            self.avg_read_latency_ns[level].store(new_avg, .release);
        }

        pub fn updateWriteLatency(self: *LevelStats, level: usize, latency_ns: u64) void {
            if (level >= self.avg_write_latency_ns.len) return;
            const old_avg = self.avg_write_latency_ns[level].load(.acquire);
            const new_avg = if (old_avg == 0) latency_ns else (old_avg * 3 + latency_ns) / 4;
            self.avg_write_latency_ns[level].store(new_avg, .release);
        }

        pub fn setCustomMetric(self: *LevelStats, key: []const u8, value: u64) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            try self.custom_metrics.put(key, value);
        }

        pub fn getCustomMetric(self: *LevelStats, key: []const u8) ?u64 {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.custom_metrics.get(key);
        }

        pub fn getTotalBytesWritten(self: *LevelStats, level: usize) u64 {
            if (level >= self.bytes_written.len) return 0;
            return self.bytes_written[level].load(.acquire);
        }

        pub fn getTotalBytesRead(self: *LevelStats, level: usize) u64 {
            if (level >= self.bytes_read.len) return 0;
            return self.bytes_read[level].load(.acquire);
        }

        pub fn getFilesCount(self: *LevelStats, level: usize) u32 {
            if (level >= self.files_count.len) return 0;
            return self.files_count[level].load(.acquire);
        }

        pub fn getLastCompactionTime(self: *LevelStats, level: usize) i64 {
            if (level >= self.last_compaction.len) return 0;
            return self.last_compaction[level].load(.acquire);
        }

        pub fn getCompactionCount(self: *LevelStats, level: usize) u32 {
            if (level >= self.compaction_count.len) return 0;
            return self.compaction_count[level].load(.acquire);
        }

        pub fn getReadLatency(self: *LevelStats, level: usize) u64 {
            if (level >= self.avg_read_latency_ns.len) return 0;
            return self.avg_read_latency_ns[level].load(.acquire);
        }

        pub fn getWriteLatency(self: *LevelStats, level: usize) u64 {
            if (level >= self.avg_write_latency_ns.len) return 0;
            return self.avg_write_latency_ns[level].load(.acquire);
        }

        pub fn resetStats(self: *LevelStats, level: usize) void {
            if (level >= self.bytes_written.len) return;
            self.bytes_written[level].store(0, .release);
            self.bytes_read[level].store(0, .release);
            // Don't reset files_count as it represents the current state
            self.total_keys[level].store(0, .release);
            // Don't reset last_compaction time
            // Don't reset compaction_count as it's historical
            self.avg_read_latency_ns[level].store(0, .release);
            self.avg_write_latency_ns[level].store(0, .release);
        }

        pub fn printStats(self: *LevelStats) void {
            std.debug.print("=== LSM Tree Stats ===\n", .{});

            // Calculate totals for summary
            var total_files: u32 = 0;
            var total_keys: u64 = 0;
            var total_bytes_written: u64 = 0;
            var total_bytes_read: u64 = 0;
            var total_compactions: u32 = 0;

            const max_levels = @min(self.bytes_written.len, 10);

            // Print header
            std.debug.print("Level | Files | Keys | Written | Read | Compactions | Latency(ms)\n", .{});
            std.debug.print("------+-------+------+---------+------+-------------+------------\n", .{});

            // Static buffers for formatting to avoid memory issues
            var written_buf: [32]u8 = undefined;
            var read_buf: [32]u8 = undefined;
            var key_buf: [32]u8 = undefined;

            for (0..max_levels) |level| {
                const files = self.getFilesCount(level);
                const keys = self.total_keys[level].load(.acquire);
                const bytes_written = self.getTotalBytesWritten(level);
                const bytes_read = self.getTotalBytesRead(level);
                const comp_count = self.getCompactionCount(level);
                const read_latency = self.getReadLatency(level);
                const write_latency = self.getWriteLatency(level);
                const avg_latency_ns = if (read_latency > 0 and write_latency > 0)
                    (read_latency + write_latency) / 2
                else if (read_latency > 0) read_latency else write_latency;
                // Convert to milliseconds
                const avg_latency_ms = @as(f64, @floatFromInt(avg_latency_ns)) / 1_000_000.0;

                // Update totals
                total_files += files;
                total_keys += keys;
                total_bytes_written += bytes_written;
                total_bytes_read += bytes_read;
                total_compactions += comp_count;

                // Format bytes in human-readable form with dedicated buffers
                const written_str = formatBytesWithBuffer(bytes_written, &written_buf);
                const read_str = formatBytesWithBuffer(bytes_read, &read_buf);

                // Print row in table format with right-aligned values
                std.debug.print("{d:4} | {d:5} | {d:4} | {s:>7} | {s:>4} | {d:11} | {d:6.2}\n", .{ level, files, keys, written_str, read_str, comp_count, avg_latency_ms });
            }

            // Print summary line
            std.debug.print("------+-------+------+---------+------+-------------+------------\n", .{});

            // Format totals with dedicated buffers
            const total_written_str = formatBytesWithBuffer(total_bytes_written, &written_buf);
            const total_read_str = formatBytesWithBuffer(total_bytes_read, &read_buf);

            std.debug.print("Total | {d:5} | {d:4} | {s:>7} | {s:>4} | {d:11} | -\n", .{ total_files, total_keys, total_written_str, total_read_str, total_compactions });

            // Print additional useful metrics in a more compact format
            std.debug.print("\n", .{});

            if (self.getCustomMetric("last_compaction_duration_ns")) |duration| {
                const duration_ms = @as(f64, @floatFromInt(duration)) / 1_000_000.0;
                std.debug.print("Last compaction: {d:.2} ms | ", .{duration_ms});
            }

            // Calculate and print efficiency metrics
            if (total_bytes_written > 0) {
                const read_write_ratio = if (total_bytes_read > 0)
                    @as(f64, @floatFromInt(total_bytes_read)) /
                        @as(f64, @floatFromInt(total_bytes_written))
                else
                    0.0;
                std.debug.print("R/W ratio: {d:.2} | ", .{read_write_ratio});
            }

            if (total_keys > 0 and total_bytes_written > 0) {
                const bytes_per_key = @as(f64, @floatFromInt(total_bytes_written)) /
                    @as(f64, @floatFromInt(total_keys));
                const bytes_per_key_str = formatBytesWithBuffer(@intFromFloat(bytes_per_key), &key_buf);
                std.debug.print("Avg per key: {s}", .{bytes_per_key_str});
            }

            std.debug.print("\n=====================\n", .{});
        }

        // Improved helper function to format bytes in human-readable form
        fn formatBytesWithBuffer(bytes: u64, buf: []u8) []const u8 {
            if (bytes == 0) {
                return "0B";
            }

            if (bytes < 1024) {
                return std.fmt.bufPrint(buf, "{d}B", .{bytes}) catch "??B";
            } else if (bytes < 1024 * 1024) {
                const kb = @as(f64, @floatFromInt(bytes)) / 1024.0;
                return std.fmt.bufPrint(buf, "{d:.1}K", .{kb}) catch "??K";
            } else if (bytes < 1024 * 1024 * 1024) {
                const mb = @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
                return std.fmt.bufPrint(buf, "{d:.1}M", .{mb}) catch "??M";
            } else {
                const gb = @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0 * 1024.0);
                return std.fmt.bufPrint(buf, "{d:.1}G", .{gb}) catch "??G";
            }
        }
    };

    pub const CompactionStrategyType = enum {
        simple,
        tiered,
    };

    // Compaction strategy interface
    pub const CompactionStrategy = struct {
        ptr: *anyopaque,
        compactFn: *const fn (ctx: *anyopaque, tm: *SSTableStore, level: usize) anyerror!void,
        deinitFn: *const fn (ctx: *anyopaque, alloc: Allocator) void,

        pub fn init(
            pointer: anytype,
            comptime compactFunc: fn (ctx: @TypeOf(pointer), tm: *SSTableStore, level: usize) anyerror!void,
            comptime deinitFunc: fn (ctx: @TypeOf(pointer), alloc: Allocator) void,
        ) CompactionStrategy {
            const Ptr = @TypeOf(pointer);
            const ptr_info = @typeInfo(Ptr);

            if (ptr_info != .Pointer) @compileError("Expected pointer, got " ++ @typeName(Ptr));
            if (ptr_info.Pointer.size != .One) @compileError("Expected single-item pointer, got " ++ @typeName(Ptr));

            // Create type-erased functions that handle the pointer type correctly
            const GenericFunctions = struct {
                fn compact(ctx: *anyopaque, tm: *SSTableStore, level: usize) !void {
                    const self = @as(Ptr, @ptrCast(@alignCast(ctx)));
                    return @call(.always_inline, compactFunc, .{ self, tm, level });
                }

                fn deinit(ctx: *anyopaque, alloc: Allocator) void {
                    const self = @as(Ptr, @ptrCast(@alignCast(ctx)));
                    @call(.always_inline, deinitFunc, .{ self, alloc });
                }
            };

            return .{
                .ptr = pointer,
                .compactFn = GenericFunctions.compact,
                .deinitFn = GenericFunctions.deinit,
            };
        }

        pub fn deinit(self: CompactionStrategy, alloc: Allocator) void {
            self.deinitFn(self.ptr, alloc);
        }

        pub fn compact(self: CompactionStrategy, tm: *SSTableStore, level: usize) !void {
            return self.compactFn(self.ptr, tm, level);
        }
    };

    pub const NoopCompactionStrategy = struct {
        pub fn init(alloc: Allocator) NoopCompactionStrategy {
            _ = alloc;
            return .{};
        }

        pub fn compact(self: *NoopCompactionStrategy, tm: *SSTableStore, level: usize) !void {
            _ = self;
            _ = tm;
            std.log.debug("noop compaction on level {d}", .{level});
        }

        pub fn deinit(self: *NoopCompactionStrategy, alloc: Allocator) void {
            alloc.destroy(self);
        }
    };

    pub const SimpleCompactionStrategy = struct {
        alloc: Allocator,

        pub fn init(alloc: Allocator) SimpleCompactionStrategy {
            return .{ .alloc = alloc };
        }

        pub fn deinit(self: *SimpleCompactionStrategy, alloc: Allocator) void {
            alloc.destroy(self);
        }

        pub fn compact(self: *SimpleCompactionStrategy, tm: *SSTableStore, level: usize) !void {
            if (level >= tm.num_levels - 1) {
                return;
            }

            const tables = tm.get(level);
            if (tables.len == 0) {
                return;
            }

            const start_time = std.time.nanoTimestamp();

            const next_level = level + 1;

            var new_table = try SSTable.init(self.alloc, next_level, tm.opts);
            errdefer {
                new_table.deinit();
                self.alloc.destroy(new_table);
            }

            var count: usize = 0;
            var bytes_read: u64 = 0;

            for (tables) |table| {
                bytes_read += table.block.size();

                var it = try table.iterator(self.alloc);
                defer it.deinit();

                while (it.next()) |kv| {
                    _ = try new_table.write(kv);
                    count += 1;
                }
            }

            if (count > 0) {
                var arena = std.heap.ArenaAllocator.init(self.alloc);
                defer arena.deinit();

                const malloc = arena.allocator();

                try new_table.block.freeze();

                const sz = new_table.block.size();

                const filename = try std.fmt.allocPrint(
                    malloc,
                    "{s}/{s}.dat",
                    .{ new_table.data_dir, new_table.id },
                );

                const out_file = try file_utils.openAndTruncate(filename, sz);
                try new_table.open(out_file);

                _ = new_table.block.flush(&new_table.stream.buf) catch |err| {
                    @panic(@errorName(err));
                };

                new_table.file.sync() catch |err| @panic(@errorName(err));

                new_table.mutable = false;

                try tm.add(new_table, next_level);

                var tables_to_remove = try std.ArrayList(*SSTable).initCapacity(
                    malloc,
                    tables.len,
                );
                defer tables_to_remove.deinit();

                for (tables) |table| {
                    try tables_to_remove.append(table);
                }

                for (tables_to_remove.items) |table| {
                    const fln = try std.fmt.allocPrint(
                        malloc,
                        "{s}/{s}.dat",
                        .{ table.data_dir, table.id },
                    );

                    table.deinit();
                    self.alloc.destroy(table);

                    try file_utils.delete(fln);
                }

                // TODO: Update thread safety
                tm.levels.items[level].clearAndFree();
                _ = tm.stats.files_count[level].store(0, .monotonic);

                tm.stats.recordBytesRead(level, bytes_read);

                const end_time = std.time.nanoTimestamp();
                const latency = @as(u64, @intCast(end_time - start_time));

                std.log.info(
                    "[simple] compacted {d} keys from level {d} to level {d} {d:.2} ms",
                    .{ count, level, next_level, latency / std.time.ns_per_ms },
                );
                try tm.stats.setCustomMetric("last_compaction_duration_ns", latency);
            } else {
                new_table.deinit();
                self.alloc.destroy(new_table);
            }
        }
    };

    pub const TieredCompactionStrategy = struct {
        alloc: Allocator,

        pub fn init(alloc: Allocator) TieredCompactionStrategy {
            return .{ .alloc = alloc };
        }

        pub fn deinit(self: *TieredCompactionStrategy, alloc: Allocator) void {
            alloc.destroy(self);
        }

        pub fn compact(self: *TieredCompactionStrategy, tm: *SSTableStore, level: usize) !void {
            _ = self;

            if (level >= tm.num_levels - 1) {
                return;
            }

            std.log.debug("[tiered] compacted {d} keys from level {d} to level {d}\n", .{ 0, level, 1 });
        }
    };

    pub fn compact(self: *Self, level: usize) !void {
        try self.compaction_strategy.compact(self, level);
        self.stats.recordCompaction(level);
    }

    pub fn getLevelStats(self: *Self, level: usize) ?LevelStatsView {
        if (level >= self.num_levels) return null;

        return LevelStatsView{
            .bytes_written = self.stats.getTotalBytesWritten(level),
            .bytes_read = self.stats.getTotalBytesRead(level),
            .files_count = self.stats.getFilesCount(level),
            .total_keys = self.stats.total_keys[level].load(.acquire),
            .last_compaction = self.stats.getLastCompactionTime(level),
            .compaction_count = self.stats.getCompactionCount(level),
            .avg_read_latency_ns = self.stats.getReadLatency(level),
            .avg_write_latency_ns = self.stats.getWriteLatency(level),
        };
    }

    pub const LevelStatsView = struct {
        bytes_written: u64,
        bytes_read: u64,
        files_count: u32,
        total_keys: u64,
        last_compaction: i64,
        compaction_count: u32,
        avg_read_latency_ns: u64,
        avg_write_latency_ns: u64,
    };

    pub fn add(self: *Self, st: *SSTable, level: usize) !void {
        if (level >= self.num_levels) {
            return error.InvalidLevel;
        }

        // TODO: Update thread safety
        try self.levels.items[level].append(st);

        _ = self.stats.files_count[level].fetchAdd(1, .monotonic);

        self.stats.recordBytesWritten(level, st.block.size());
        self.stats.recordKeysWritten(level, st.block.count);
    }

    pub fn get(self: Self, level: usize) []const *SSTable {
        // TODO: Update thread safety
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
        const start_time = std.time.nanoTimestamp();

        var sstable = try SSTable.init(alloc, 0, self.opts);
        errdefer alloc.destroy(sstable);
        errdefer sstable.deinit();

        mtable.freeze();

        var it = try mtable.iterator(alloc);
        defer it.deinit();

        var keys_written: u64 = 0;
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
            keys_written += 1;
        }

        if (!sstable.connected and sstable.mutable) {
            try sstable.block.freeze();

            const sz = sstable.block.size();

            const filename = try std.fmt.allocPrint(
                alloc,
                "{s}/{s}.dat",
                .{ sstable.data_dir, sstable.id },
            );
            defer alloc.free(filename);

            const out_file = try file_utils.openAndTruncate(filename, sz);
            try sstable.open(out_file);

            _ = sstable.block.flush(&sstable.stream.buf) catch |err| {
                @panic(@errorName(err));
            };

            sstable.file.sync() catch |err| @panic(@errorName(err));

            sstable.mutable = false;
        }

        mtable.isFlushed.store(true, .release);

        try self.add(sstable, 0);

        const end_time = std.time.nanoTimestamp();
        const latency = @as(u64, @intCast(end_time - start_time));

        self.stats.updateWriteLatency(0, latency);
        self.stats.recordKeysWritten(0, keys_written);
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

    pub fn init(alloc: Allocator, level: usize, opts: Opts) !*Self {
        const block = try Block.init(alloc, opts.sst_capacity);
        errdefer {
            block.deinit();
            alloc.destroy(block);
        }

        const sstid = try std.fmt.allocPrint(
            alloc,
            "level-{d}_{d}",
            .{ level, std.time.milliTimestamp() },
        );
        errdefer alloc.free(sstid);

        const st = try alloc.create(Self);
        st.* = .{
            .alloc = alloc,
            .block = block,
            .capacity = opts.sst_capacity,
            .data_dir = opts.data_dir,
            .id = sstid,
            .mutable = true,
            .connected = false,
            .file = undefined,
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
        self.alloc.free(self.id);
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

                // Record read latency if we have access to stats
                // This is a bit of a hack since we don't have direct access to the store's stats
                // In a real implementation, we might want to pass the stats object or use a global context
                // const start_time = std.time.nanoTimestamp();
                // const end_time = std.time.nanoTimestamp();
                // const latency = @as(u64, @intCast(end_time - start_time));

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
    var st = try SSTable.init(alloc, 0, dopts);
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
}

test "CompactionStrategies" {
    const testing = std.testing;
    var talloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const pathname = try testDir.dir.realpathAlloc(talloc, ".");
    defer talloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    const CompactionStrategy = SSTableStore.CompactionStrategy;

    const createTestSSTable = struct {
        fn create(a: Allocator, level: usize, opts: Opts) !*SSTable {
            var table = try SSTable.init(a, level, opts);
            errdefer {
                table.deinit();
                a.destroy(table);
            }

            // Add some test data
            _ = try table.write(KV.init("key1", "value1"));
            _ = try table.write(KV.init("key2", "value2"));
            _ = try table.write(KV.init("key3", "value3"));

            return table;
        }
    }.create;

    const verifyCompactionResults = struct {
        fn verify(a: Allocator, tm: *SSTableStore, level: usize, expectedCount: usize) !void {
            const tables = tm.get(level);
            try testing.expectEqual(expectedCount, tables.len);

            if (expectedCount > 0) {
                // Verify the data in the compacted table
                var it = try tm.iterator(a);
                defer it.deinit();

                var count: usize = 0;
                while (it.next()) |_| {
                    count += 1;
                }

                // We should have at least the number of keys we inserted
                try testing.expect(count >= 3);
            }
        }
    }.verify;

    // Test SimpleCompactionStrategy
    {
        var dopts = options.defaultOpts();
        dopts.data_dir = pathname;
        dopts.num_levels = 3;

        var tm = try SSTableStore.init(talloc, dopts);
        defer tm.deinit(talloc);

        const table1 = try createTestSSTable(talloc, 0, dopts);
        try tm.add(table1, 0);

        const table2 = try createTestSSTable(talloc, 0, dopts);
        try tm.add(table2, 0);

        try tm.compact(0);

        // level 0 should be empty, level 1 should have 1 table
        try verifyCompactionResults(talloc, &tm, 0, 0);
        try verifyCompactionResults(talloc, &tm, 1, 1);
    }

    // Test TieredCompactionStrategy
    {
        var dopts = options.defaultOpts();
        dopts.compaction_strategy = .tiered;
        dopts.data_dir = pathname;
        dopts.num_levels = 3;

        var tm = try SSTableStore.init(talloc, dopts);
        defer tm.deinit(talloc);

        const table1 = try createTestSSTable(talloc, 0, dopts);
        try tm.add(table1, 0);

        const table2 = try createTestSSTable(talloc, 0, dopts);
        try tm.add(table2, 0);

        try tm.compact(0);

        // Since TieredCompactionStrategy is just a stub, we expect no changes
        try verifyCompactionResults(talloc, &tm, 0, 2);
        try verifyCompactionResults(talloc, &tm, 1, 0);
    }

    // Test with a custom strategy (demonstrating extensibility)
    {
        const CustomCompactionStrategy = struct {
            alloc: Allocator,

            pub fn init(alloc: Allocator) @This() {
                return .{ .alloc = alloc };
            }

            fn deinit(self: *@This(), alloc: Allocator) void {
                alloc.destroy(self);
            }

            pub fn compact(self: *@This(), tm: *SSTableStore, level: usize) !void {
                _ = self;

                // Simple implementation that just moves tables to the next level without merging
                if (level >= tm.num_levels - 1) {
                    return;
                }

                const tables = tm.get(level);
                if (tables.len == 0) {
                    return;
                }

                // Move all tables to the next level
                const next_level = level + 1;
                for (tables) |table| {
                    try tm.add(table, next_level);
                }

                tm.levels.items[level].clearRetainingCapacity();
            }
        };

        var dopts = options.defaultOpts();
        dopts.data_dir = pathname;
        dopts.num_levels = 3;

        var tm = try SSTableStore.init(talloc, dopts);
        defer tm.deinit(talloc);

        const table1 = try createTestSSTable(talloc, 0, dopts);
        try tm.add(table1, 0);

        const table2 = try createTestSSTable(talloc, 0, dopts);
        try tm.add(table2, 0);

        const custom_strategy = try talloc.create(CustomCompactionStrategy);
        custom_strategy.* = CustomCompactionStrategy.init(talloc);

        tm.setCompactionStrategy(
            talloc,
            CompactionStrategy.init(
                custom_strategy,
                CustomCompactionStrategy.compact,
                CustomCompactionStrategy.deinit,
            ),
        );

        try tm.compact(0);

        // level 0 should be empty, level 1 should have 2 tables
        try verifyCompactionResults(talloc, &tm, 0, 0);
        try verifyCompactionResults(talloc, &tm, 1, 2);
    }
}
