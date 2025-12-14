const std = @import("std");

const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");
const mt = @import("memtable.zig");
const options = @import("opts.zig");
const file_utils = @import("file.zig");
const sst = @import("sstable.zig");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const SSTable = sst.SSTable;
const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const Memtable = mt.Memtable;
const MergeIterator = iter.MergeIterator;
const Opts = options.Opts;

const bufPrint = std.fmt.bufPrint;
const parseInt = std.fmt.parseInt;
const startsWith = std.mem.startsWith;

pub const SSTableStore = struct {
    alloc: Allocator,
    levels: ArrayList(ArrayList(*SSTable)),
    opts: Opts,
    num_levels: usize,
    compaction_strategy: CompactionStrategy,
    stats: LevelStats,
    mutex: std.Thread.Mutex = .{},

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

        var levels = try ArrayList(ArrayList(*SSTable)).initCapacity(alloc, 256);

        var i: usize = 0;
        while (i < num_levels) : (i += 1) {
            try levels.append(alloc, try ArrayList(*SSTable).initCapacity(alloc, 256));
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
            .alloc = alloc,
            .levels = levels,
            .opts = opts,
            .num_levels = num_levels,
            .compaction_strategy = strategy,
            .stats = LevelStats.init(alloc, num_levels),
        };
    }

    pub fn clearAndFree(self: *Self, alloc: Allocator, level: usize) !void {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        const malloc = arena.allocator();

        var tables_to_release = try ArrayList(*SSTable).initCapacity(malloc, 256);
        defer tables_to_release.deinit(malloc);

        try tables_to_release.appendSlice(malloc, self.get(level));
        for (tables_to_release.items) |table| {
            const fln = try std.fmt.allocPrint(
                malloc,
                "{s}/{s}.dat",
                .{ table.data_dir, table.id },
            );

            table.deinit();
            alloc.destroy(table);

            file_utils.delete(fln) catch |err| {
                std.log.warn("file deletion failed for {s} {s}", .{ fln, @errorName(err) });
            };
        }

        self.mutex.lock();
        self.levels.items[level].clearRetainingCapacity();
        self.mutex.unlock();

        _ = self.stats.files_count[level].store(0, .monotonic);
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        {
            self.mutex.lock();
            defer self.mutex.unlock();

            for (self.levels.items) |*level| {
                for (level.items) |sstable| {
                    sstable.deinit();
                    alloc.destroy(sstable);
                }
                level.deinit(alloc);
            }
        }

        self.compaction_strategy.deinit(alloc);
        self.levels.deinit(alloc);
        self.stats.printStats();
        self.stats.deinit();
    }

    pub fn open(self: *Self, alloc: Allocator) !void {
        var data_dir = try std.fs.openDirAbsolute(self.opts.data_dir, .{ .iterate = true });
        defer data_dir.close();

        var dir_iter = try data_dir.walk(alloc);
        defer dir_iter.deinit();

        while (try dir_iter.next()) |f| {
            if (std.mem.startsWith(u8, f.basename, ".") or
                std.mem.startsWith(u8, f.basename, "wal_") or
                std.mem.endsWith(u8, f.basename, "_temp.dat") or
                !std.mem.endsWith(u8, f.basename, ".dat"))
            {
                continue;
            }

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

            // Extract level from filename (remove .dat extension first)
            const basename_no_ext = f.basename[0 .. f.basename.len - 4]; // Remove ".dat"
            var filename_parts = std.mem.splitSequence(u8, basename_no_ext, "_");
            const level_name = filename_parts.first();
            const level = try Level.fromString(level_name);

            var sstable = try SSTable.init(alloc, level, self.opts);
            try sstable.open(data_file);

            _ = sstable.release();
            try self.add(sstable, level);

            alloc.free(nxt_file);
        }
    }

    pub fn setCompactionStrategy(self: *Self, alloc: Allocator, strategy: CompactionStrategy) void {
        self.compaction_strategy.deinit(alloc);
        self.compaction_strategy = strategy;
    }

    pub const LevelStats = struct {
        alloc: Allocator,
        bytes_written: []std.atomic.Value(u64),
        bytes_read: []std.atomic.Value(u64),
        files_count: []std.atomic.Value(u32),
        total_keys: []std.atomic.Value(u64),

        last_compaction: []std.atomic.Value(i64),
        compaction_count: []std.atomic.Value(u32),

        avg_read_latency_ns: []std.atomic.Value(u64),
        avg_write_latency_ns: []std.atomic.Value(u64),

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
            const total_written_str = formatBytesWithBuffer(
                total_bytes_written,
                &written_buf,
            );
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
            // const ptr_info = @typeInfo(Ptr);

            // if (ptr_info != .Pointer) @compileError("Expected pointer, got " ++ @typeName(Ptr));
            // if (ptr_info.Pointer.size != .One) @compileError("Expected single-item pointer, got " ++ @typeName(Ptr));

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

            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            const temp_alloc = arena.allocator();

            var retained_tables = try std.ArrayList(*SSTable).initCapacity(temp_alloc, 256);
            defer {
                for (retained_tables.items) |table| {
                    _ = table.release();
                }
            }

            var current_level_bytes: usize = 0;
            for (tables) |table| {
                current_level_bytes += table.block.size();
                table.retain();
                try retained_tables.append(temp_alloc, table);
            }

            const metadata_overhead = 1024; // Space for headers, offsets, etc.
            const safety_buffer = current_level_bytes / 4; // 25% buffer for safety
            const estimated_size = current_level_bytes + metadata_overhead + safety_buffer;

            const start_time = std.time.nanoTimestamp();

            const next_level = level + 1;

            var new_table = try SSTable.init(self.alloc, next_level, tm.opts);
            errdefer {
                new_table.deinit();
                self.alloc.destroy(new_table);
            }

            const filename = try std.fmt.allocPrint(
                temp_alloc,
                "{s}/{s}.dat",
                .{ new_table.data_dir, new_table.id },
            );

            const out_file = try file_utils.openAndTruncate(filename, estimated_size);
            try new_table.open(out_file);

            var keys_compacted: usize = 0;
            var bytes_read: u64 = 0;

            var merger = try MergeIterator(KV, keyvalue.compare).init(temp_alloc);
            defer MergeIterator(KV, keyvalue.compare).deinit(&merger);

            for (tables) |table| {
                const table_iterator = try table.iterator(temp_alloc);
                try merger.add(table_iterator);
            }

            var merge_iter = merger.iterator();

            while (merge_iter.next()) |nxt| {
                var kv_copy = try nxt.clone(temp_alloc);
                defer kv_copy.deinit(temp_alloc);

                _ = try new_table.write(kv_copy);

                keys_compacted += 1;
                bytes_read += kv_copy.len();
            }

            if (keys_compacted > 0) {
                try new_table.block.flush();

                new_table.file.sync() catch |err| {
                    std.log.err(
                        "not able to file sync {s} for file {s}",
                        .{ @errorName(err), filename },
                    );
                    return err;
                };

                new_table.mutable = false;

                _ = new_table.release();
                try tm.add(new_table, next_level);

                try tm.clearAndFree(self.alloc, level);

                tm.stats.recordBytesRead(level, bytes_read);

                const end_time = std.time.nanoTimestamp();
                const latency = @as(u64, @intCast(end_time - start_time));

                const duration_ms = @as(f64, @floatFromInt(latency)) / std.time.ns_per_ms;

                std.log.info(
                    "[simple] compacted {d} keys from level {d} to level {d} in {d:.2} ms",
                    .{ keys_compacted, level, next_level, duration_ms },
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

        {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Increment reference count when adding to a level
            st.retain();
            try self.levels.items[level].append(self.alloc, st);
        }

        _ = self.stats.files_count[level].fetchAdd(1, .monotonic);

        self.stats.recordBytesWritten(level, st.block.size());
        self.stats.recordKeysWritten(level, st.block.count);
    }

    pub fn get(self: *Self, level: usize) []const *SSTable {
        if (level >= self.num_levels) {
            return &[_]*SSTable{};
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        return self.levels.items[level].items;
    }

    pub fn read(self: *Self, key: []const u8) !?KV {
        self.mutex.lock();
        defer self.mutex.unlock();

        // TODO: Update to make sure we iterate over the most recent sstable for
        // each level
        for (0..self.num_levels) |level| {
            for (self.levels.items[level].items) |sstable| {
                if (try sstable.xread(key)) |kv| {
                    return kv;
                }
            }
        }

        return null;
    }

    const LevelIterator = struct {
        alloc: Allocator,
        iter: Iterator(KV),
        merger: *MergeIterator(KV, keyvalue.compare),
        used_tables: ArrayList(*SSTable),

        pub fn deinit(ctx: *anyopaque) void {
            const self: *LevelIterator = @ptrCast(@alignCast(ctx));
            self.iter.deinit();

            // Release all tables we were using
            for (self.used_tables.items) |table| {
                _ = table.release();
            }
            self.used_tables.deinit(self.alloc);

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

        // Create a list to track all tables we're using
        var used_tables = try ArrayList(*SSTable).initCapacity(alloc, 256);
        errdefer used_tables.deinit(alloc);

        var level_idx: usize = 0;
        while (level_idx < self.num_levels) : (level_idx += 1) {
            for (self.get(level_idx)) |sstable| {
                // Retain the table while the iterator is active
                sstable.retain();
                try used_tables.append(alloc, sstable);

                const siter = try sstable.iterator(alloc);
                try merger.add(siter);
            }
        }

        const it = try alloc.create(LevelIterator);
        it.* = .{
            .alloc = alloc,
            .merger = merger,
            .iter = merger.iterator(),
            .used_tables = used_tables,
        };

        return Iterator(KV).init(it, LevelIterator.next, LevelIterator.deinit);
    }

    pub fn flush(self: *Self, alloc: Allocator, mtable: *Memtable) !void {
        if (mtable.size() == 0) return;

        const start_time = std.time.nanoTimestamp();

        var sstable = try SSTable.init(alloc, 0, self.opts);
        errdefer {
            sstable.deinit();
            alloc.destroy(sstable);
        }

        const filename = try std.fmt.allocPrint(
            alloc,
            "{s}/{s}.dat",
            .{ sstable.data_dir, sstable.id },
        );
        defer alloc.free(filename);

        const sz = mtable.size() * 10;
        const out_file = try file_utils.openWithCapacity(filename, sz);

        try sstable.open(out_file);

        var it = try mtable.iterator(alloc);
        defer it.deinit();

        var keys_written: u64 = 0;
        while (it.next()) |nxt| {
            _ = sstable.write(nxt) catch |err| switch (err) {
                error.DuplicateError => continue,
                else => {
                    std.log.err(
                        "memtable not able to write to sstable for key {s}: {s}",
                        .{ nxt.key, @errorName(err) },
                    );
                    return err;
                },
            };

            keys_written += 1;
        }

        if (keys_written > 0) {
            try sstable.block.flush();

            sstable.file.sync() catch |err| {
                std.log.err(
                    "file synced failed {s} {s}",
                    .{ sstable.id, @errorName(err) },
                );
                return err;
            };

            sstable.mutable = false;
        }

        mtable.flush();

        _ = sstable.release();
        try self.add(sstable, 0);

        const end_time = std.time.nanoTimestamp();
        const latency = @as(u64, @intCast(end_time - start_time));

        self.stats.updateWriteLatency(0, latency);
        self.stats.recordKeysWritten(0, keys_written);
    }
};

test "SSTableStore flush and compaction" {
    const testing = std.testing;
    var alloc = testing.allocator;
    const testDir = testing.tmpDir(.{});

    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    const allocator = arena.allocator();

    var dopts = options.defaultOpts();
    dopts.data_dir = pathname;
    dopts.num_levels = 3;

    var store = try SSTableStore.init(allocator, dopts);
    defer store.deinit(allocator);

    try store.open(allocator);

    var memtable = try Memtable.init(allocator, "level-0");
    defer allocator.destroy(memtable);
    defer memtable.deinit(allocator);

    try memtable.put(try KV.initOwned(allocator, "flush_key1", "flush_value1"));
    try memtable.put(try KV.initOwned(allocator, "flush_key2", "flush_value2"));
    try memtable.put(try KV.initOwned(allocator, "flush_key3", "flush_value3"));

    // Flush memtable to SSTable
    {
        try store.flush(allocator, memtable);

        try testing.expect(memtable.flushed());
        try testing.expectEqual(@as(usize, 1), store.get(0).len);
    }

    // Read back the data
    {
        const kv = try store.read("flush_key1");
        try testing.expectEqualStrings("flush_value1", kv.?.value);
    }

    // Test store iterator
    {
        var it = try store.iterator(allocator);
        defer it.deinit();

        var count: usize = 0;
        while (it.next()) |_| {
            count += 1;
        }
        try testing.expectEqual(@as(usize, 3), count);
    }

    // Test compaction
    {
        try store.compact(0);

        // Level 0 should be empty, level 1 should have data
        try testing.expectEqual(@as(usize, 0), store.get(0).len);
        try testing.expectEqual(@as(usize, 1), store.get(1).len);

        // Verify data is still accessible after compaction
        const kv = try store.read("flush_key2");
        try testing.expectEqualStrings("flush_value2", kv.?.value);
    }
}

test "SSTableStore open" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const testDir = testing.tmpDir(.{});
    const pathname = try testDir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer testDir.dir.deleteTree(pathname) catch {};

    var dopts = options.defaultOpts();
    dopts.data_dir = pathname;
    dopts.num_levels = 3;

    // Create and persist some SSTables
    {
        var store = try SSTableStore.init(alloc, dopts);
        defer store.deinit(alloc);

        // Create and add tables to different levels
        var table0 = try SSTable.init(alloc, 0, dopts);

        // Create a temporary file for the table
        const filename0 = try std.fmt.allocPrint(
            alloc,
            "{s}/{s}.dat",
            .{ table0.data_dir, table0.id },
        );
        defer alloc.free(filename0);

        const sz0 = 1024; // Initial size
        const out_file0 = try file_utils.openWithCapacity(filename0, sz0);
        try table0.open(out_file0);

        _ = try table0.write(try KV.initOwned(alloc, "key0", "value0"));
        _ = try table0.write(try KV.initOwned(alloc, "key1", "value1"));

        try store.add(table0, 0);
        _ = table0.release();

        var table1 = try SSTable.init(alloc, 1, dopts);

        // Create a temporary file for table1
        const filename1 = try std.fmt.allocPrint(
            alloc,
            "{s}/{s}.dat",
            .{ table1.data_dir, table1.id },
        );
        defer alloc.free(filename1);

        const sz1 = 1024; // Initial size
        const out_file1 = try file_utils.openWithCapacity(filename1, sz1);
        try table1.open(out_file1);

        _ = try table1.write(try KV.initOwned(alloc, "key1", "value1"));

        try store.add(table1, 1);
        _ = table1.release();

        // Persist tables to disk
        for (0..2) |level| {
            for (store.get(level)) |table| {
                try table.block.flush();

                try table.file.sync();

                table.mutable = false;
            }
        }
    }

    // Now create a new store and test the open function
    {
        var store = try SSTableStore.init(alloc, dopts);
        defer store.deinit(alloc);

        // Open the store to load tables from disk
        try store.open(alloc);

        // Verify tables were loaded correctly
        try testing.expectEqual(@as(usize, 1), store.get(0).len);
        try testing.expectEqual(@as(usize, 1), store.get(1).len);

        // Verify data in the tables
        var kv = try store.read("key0");
        try testing.expectEqualStrings("value0", kv.?.value);

        kv = try store.read("key1");
        try testing.expectEqualStrings("value1", kv.?.value);
    }
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

            // Create a file for the table
            const filename = try std.fmt.allocPrint(
                a,
                "{s}/{s}.dat",
                .{ table.data_dir, table.id },
            );
            defer a.free(filename);

            const sz = 1024; // Initial size
            const out_file = try file_utils.openAndTruncate(filename, sz);
            try table.open(out_file);
            errdefer {
                table.deinit();
                a.destroy(table);
            }

            // Add some test data
            _ = try table.write(try KV.initOwned(a, "key1", "value1"));
            _ = try table.write(try KV.initOwned(a, "key2", "value2"));
            _ = try table.write(try KV.initOwned(a, "key3", "value3"));

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

    var arena = std.heap.ArenaAllocator.init(talloc);
    defer arena.deinit();

    // Test SimpleCompactionStrategy
    {
        const alloc = arena.allocator();

        var dopts = options.defaultOpts();
        dopts.data_dir = pathname;
        dopts.num_levels = 3;

        var tm = try SSTableStore.init(alloc, dopts);
        defer tm.deinit(alloc);

        // Create tables and add them to the store
        // The tables are now managed by the store with reference counting
        var table1 = try createTestSSTable(alloc, 0, dopts);
        try tm.add(table1, 0);
        _ = table1.release();

        var table2 = try createTestSSTable(alloc, 0, dopts);
        try tm.add(table2, 0);
        _ = table2.release();

        try tm.compact(0);

        // level 0 should be empty, level 1 should have 1 table
        try verifyCompactionResults(alloc, &tm, 0, 0);
        try verifyCompactionResults(alloc, &tm, 1, 1);
    }

    // Test TieredCompactionStrategy
    {
        const alloc = arena.allocator();

        var dopts = options.defaultOpts();
        dopts.compaction_strategy = .tiered;
        dopts.data_dir = pathname;
        dopts.num_levels = 3;

        var tm = try SSTableStore.init(alloc, dopts);
        defer tm.deinit(alloc);

        var table1 = try createTestSSTable(alloc, 0, dopts);
        try tm.add(table1, 0);
        _ = table1.release();

        var table2 = try createTestSSTable(alloc, 0, dopts);
        try tm.add(table2, 0);
        _ = table2.release();

        try tm.compact(0);

        // Since TieredCompactionStrategy is just a stub, we expect no changes
        try verifyCompactionResults(alloc, &tm, 0, 2);
        try verifyCompactionResults(alloc, &tm, 1, 0);
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

                // Simple implementation that just moves tables to the next level
                // without merging
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
                    _ = table.release();
                    try tm.add(table, next_level);
                }

                tm.levels.items[level].clearRetainingCapacity();
            }
        };

        const alloc = arena.allocator();

        var dopts = options.defaultOpts();
        dopts.data_dir = pathname;
        dopts.num_levels = 3;

        var tm = try SSTableStore.init(alloc, dopts);
        defer tm.deinit(alloc);

        var table1 = try createTestSSTable(alloc, 0, dopts);
        try tm.add(table1, 0);
        _ = table1.release();

        var table2 = try createTestSSTable(alloc, 0, dopts);
        try tm.add(table2, 0);
        _ = table2.release();

        const custom_strategy = try alloc.create(CustomCompactionStrategy);
        custom_strategy.* = CustomCompactionStrategy.init(talloc);

        tm.setCompactionStrategy(
            alloc,
            CompactionStrategy.init(
                custom_strategy,
                CustomCompactionStrategy.compact,
                CustomCompactionStrategy.deinit,
            ),
        );

        try tm.compact(0);

        // level 0 should be empty, level 1 should have 2 tables
        try verifyCompactionResults(alloc, &tm, 0, 0);
        try verifyCompactionResults(alloc, &tm, 1, 2);
    }
}
