const std = @import("std");

const PageSize = std.heap.pageSize();

// const CompactionStrategyType = store.SSTableStore.CompactionStrategyType;

pub const Opts = struct {
    data_dir: []const u8,
    sst_capacity: usize,
    wal_capacity: usize,
    num_levels: ?usize,
    // compaction_strategy: ?CompactionStrategyType = null,
    enable_agent: ?bool = null,
};

const MB = 1024 * 1024;
const KB = 1024;

pub fn defaultOpts() Opts {
    return .{
        .data_dir = ".tmp/data",
        // TODO: temp
        .sst_capacity = 256 * MB,
        // .sst_capacity = 16 * MB,
        .wal_capacity = PageSize * PageSize,
        .num_levels = 3,
        // .compaction_strategy = .simple,
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
