const std = @import("std");

const sstable = @import("sstable.zig");

const PageSize = std.mem.page_size;

const CompactionStrategyType = sstable.SSTableStore.CompactionStrategyType;

pub const Opts = struct {
    data_dir: []const u8,
    sst_capacity: usize,
    wal_capacity: usize,
    num_levels: ?usize,
    compaction_strategy: ?CompactionStrategyType = null,
};

pub fn defaultOpts() Opts {
    return .{
        .data_dir = ".tmp/data",
        // TODO: temp
        .sst_capacity = 1_000_000,
        .wal_capacity = PageSize * PageSize,
        .num_levels = 3,
        .compaction_strategy = .simple,
    };
}

pub fn withDataDirOpts(data_dir: []const u8) Opts {
    var opts = defaultOpts();
    opts.data_dir = data_dir;
    return opts;
}
