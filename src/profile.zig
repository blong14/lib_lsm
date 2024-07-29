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
    elapsed: u64,
    hit_count: u64,
};

pub const Profiler = struct {
    anchors: std.ArrayList(ProfileAnchor),
    start: u64,
    end: u64,

    pub fn init(alloc: Allocator) Profiler {
        return .{
            .anchors = std.ArrayList(ProfileAnchor).init(alloc),
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
    idx: usize,

    const Self = @This();

    pub fn init() Self {
        const current_idx = GlobalProfiler.anchors.items.len;
        GlobalProfiler.anchors.append(
            ProfileAnchor{
                .label = undefined,
                .elapsed = 0,
                .hit_count = 0,
            },
        ) catch |err| {
            @panic(@errorName(err));
        };
        return .{
            .idx = current_idx,
            .label = undefined,
            .start_time = 0,
        };
    }

    pub fn start(self: *Self, label: []const u8) void {
        self.start_time = readCPUTimer();
        self.label = label;
    }

    pub fn end(self: Self) void {
        const elapsed: u64 = readCPUTimer() - self.start_time;
        GlobalProfiler.anchors.items[self.idx].elapsed += elapsed;
        GlobalProfiler.anchors.items[self.idx].hit_count += 1;
        GlobalProfiler.anchors.items[self.idx].label = self.label;
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

    const total_time: u64 = GlobalProfiler.end - GlobalProfiler.start;
    const cpu_freq: u64 = estimateCPUFrequency();

    std.debug.print(
        "\nTotal time {}ms\n",
        .{1000 * total_time / cpu_freq},
    );

    for (GlobalProfiler.anchors.items) |anchor| {
        if (anchor.elapsed > 0) {
            const anchor_time = 1000 * anchor.elapsed / cpu_freq;
            const percent = 100 * anchor.elapsed / total_time;
            std.debug.print(
                "\t[{s}]: hit count {d} total time {}ms ({}%)\n",
                .{ anchor.label, anchor.hit_count, anchor_time, percent },
            );
        }
    }
}
