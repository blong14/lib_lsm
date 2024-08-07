const std = @import("std");

const sys = @cImport({
    @cInclude("sys/time.h");
});

const Allocator = std.mem.Allocator;

inline fn getOSTimerFreq() c_long {
    return 1_000_000;
}

pub inline fn readOSTimer() u64 {
    var value: sys.timeval = undefined;
    _ = sys.gettimeofday(&value, null);
    const result = getOSTimerFreq() * value.tv_sec + value.tv_usec;
    return @intCast(result);
}

pub inline fn readCPUTimer() u64 {
    var hi: u32 = 0;
    var low: u32 = 0;
    asm (
        \\rdtsc
        : [low] "={eax}" (low),
          [hi] "={edx}" (hi),
    );
    return (@as(u64, hi) << 32) | @as(u64, low);
}

pub fn estimateCPUFrequency() u64 {
    const ms_to_wait = 100;
    const os_freq: u64 = getOSTimerFreq();
    const cpu_start = readCPUTimer();
    const os_start = readOSTimer();
    const os_wait_time = os_freq * ms_to_wait / 1000;

    var os_end: u64 = 0;
    var os_elapsed: u64 = 0;
    while (os_elapsed < os_wait_time) {
        os_end = readOSTimer();
        os_elapsed = os_end - os_start;
    }

    const cpu_end: u64 = readCPUTimer();
    const cpu_elapsed: u64 = cpu_end - cpu_start;

    return os_freq * cpu_elapsed / os_elapsed;
}

pub const ProfileAnchor = struct {
    label: []const u8,
    parent_anchor: []const u8,
    hit_count: u64,
    elapsed_inclusive: u64, // does include children
    elapsed_exclusive: u64, // does NOT include children
    processed_bytes: u64,
};

pub const Profiler = struct {
    anchors: std.StringHashMap(ProfileAnchor),
    active_anchor: []const u8,
    start: u64,
    end: u64,

    pub fn init(alloc: Allocator) Profiler {
        return .{
            .active_anchor = "",
            .anchors = std.StringHashMap(ProfileAnchor).init(alloc),
            .start = 0,
            .end = 0,
        };
    }

    pub fn deinit(self: *Profiler) void {
        self.anchors.deinit();
        self.* = undefined;
    }
};

pub const BlockProfiler = struct {
    label: []const u8,
    start_time: u64,
    elapsed_inclusive: u64,
    bytes: u64,

    const Self = @This();

    pub fn start(label: []const u8) Self {
        const result = GlobalProfiler.anchors.getOrPut(label) catch |err| {
            @panic(@errorName(err));
        };
        if (!result.found_existing) {
            result.value_ptr.* = ProfileAnchor{
                .label = label,
                .parent_anchor = GlobalProfiler.active_anchor,
                .elapsed_inclusive = 0,
                .elapsed_exclusive = 0,
                .hit_count = 0,
                .processed_bytes = 0,
            };
        }
        const anchor = result.value_ptr.*;
        GlobalProfiler.active_anchor = anchor.label;
        return .{
            .label = label,
            .elapsed_inclusive = anchor.elapsed_inclusive,
            .start_time = readCPUTimer(),
            .bytes = 0,
        };
    }

    pub fn withBytes(self: *Self, byts: u64) void {
        self.bytes = byts;
    }

    pub fn end(self: Self) void {
        const elapsed: u64 = readCPUTimer() - self.start_time;

        var anchor = GlobalProfiler.anchors.get(self.label).?;

        if (GlobalProfiler.anchors.get(anchor.parent_anchor)) |parent| {
            var parent_anchor = parent;
            const result = @subWithOverflow(parent_anchor.elapsed_exclusive, elapsed);
            if (result[1] == 0) {
                parent_anchor.elapsed_exclusive = result[0];
            }
            GlobalProfiler.anchors.put(parent_anchor.label, parent_anchor) catch |err| {
                @panic(@errorName(err));
            };
        }

        anchor.elapsed_exclusive += elapsed;
        anchor.elapsed_inclusive = self.elapsed_inclusive + elapsed;
        anchor.hit_count += 1;
        anchor.processed_bytes += self.bytes;

        GlobalProfiler.anchors.put(self.label, anchor) catch |err| {
            @panic(@errorName(err));
        };
        GlobalProfiler.active_anchor = anchor.parent_anchor;
    }
};

var GlobalProfiler: Profiler = undefined;

pub inline fn BeginProfile(alloc: Allocator) void {
    GlobalProfiler = Profiler.init(alloc);
    GlobalProfiler.start = readCPUTimer();
}

pub inline fn EndProfile() void {
    defer GlobalProfiler.deinit();

    GlobalProfiler.end = readCPUTimer();

    const cpu_freq: u64 = estimateCPUFrequency();
    const total_time: u64 = GlobalProfiler.end - GlobalProfiler.start;

    std.debug.print(
        "\nTotal time {}ms (timer frequency: {})\n",
        .{ 1000 * total_time / cpu_freq, cpu_freq },
    );

    var iter = GlobalProfiler.anchors.iterator();
    while (iter.next()) |entry| {
        const anchor = entry.value_ptr;
        if (anchor.elapsed_inclusive > 0) {
            const anchor_time = 1000 * anchor.elapsed_exclusive / cpu_freq;
            const anchor_percent = 100 * anchor.elapsed_exclusive / total_time;

            std.debug.print(
                "\t[{s}::{s}]: hits {d} {}ms {}% of total time",
                .{
                    anchor.parent_anchor,
                    anchor.label,
                    anchor.hit_count,
                    anchor_time,
                    anchor_percent,
                },
            );

            if (anchor.elapsed_inclusive != anchor.elapsed_exclusive) {
                const anchor_time_w_children = 1000 * anchor.elapsed_inclusive / cpu_freq;
                const anchor_percent_w_children = 100 * anchor.elapsed_inclusive / total_time;

                std.debug.print(
                    " ({}ms w/ children {}% of total time)",
                    .{ anchor_time_w_children, anchor_percent_w_children },
                );
            }

            if (anchor.processed_bytes > 0) {
                const mega_byte = 1024 * 1024;
                const giga_byte = mega_byte * 1024;

                const seconds: f64 = @as(f64, @floatFromInt(1000 * anchor.elapsed_inclusive / cpu_freq)) / 1000.0;
                const bytes_per_second: f64 = @as(f64, @floatFromInt(anchor.processed_bytes)) / seconds;
                const mega_bytes: f64 = @as(f64, @floatFromInt(anchor.processed_bytes)) / mega_byte;
                const giga_bytes_per_second: f64 = bytes_per_second / giga_byte;

                std.debug.print(
                    " {d:.2}mb @ {d:.2}gb/s",
                    .{ mega_bytes, giga_bytes_per_second },
                );
            }

            std.debug.print("\n", .{});
        }
    }
}
