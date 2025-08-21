const std = @import("std");

const store = @import("store.zig");

const PageSize = std.mem.page_size;

const CompactionStrategyType = store.SSTableStore.CompactionStrategyType;

pub const Opts = struct {
    data_dir: []const u8,
    sst_capacity: usize,
    wal_capacity: usize,
    num_levels: ?usize,
    compaction_strategy: ?CompactionStrategyType = null,
    enable_agent: ?bool = null,
};

pub fn defaultOpts() Opts {
    return .{
        .data_dir = ".tmp/data",
        // TODO: temp
        .sst_capacity = 1_000_000,
        .wal_capacity = PageSize * PageSize,
        .num_levels = 3,
        .compaction_strategy = .simple,
        .enable_agent = false,
    };
}

pub fn withDataDirOpts(data_dir: []const u8) Opts {
    var opts = defaultOpts();
    opts.data_dir = data_dir;
    return opts;
}

pub fn withAgentOpts(data_dir: []const u8) Opts {
    var opts = withDataDirOpts(data_dir);
    opts.enable_agent = true;
    return opts;
}
