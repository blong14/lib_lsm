const std = @import("std");

const PageSize = std.mem.page_size;

pub const Opts = struct {
    data_dir: []const u8,
    sst_capacity: usize,
    wal_capacity: usize,
};

pub fn defaultOpts() Opts {
    return .{
        .data_dir = ".tmp/data",
        // TODO: temp
        .sst_capacity = 1_000_000,
        .wal_capacity = PageSize * PageSize,
    };
}

pub fn withDataDirOpts(data_dir: []const u8) Opts {
    var opts = defaultOpts();
    opts.data_dir = data_dir;
    return opts;
}
