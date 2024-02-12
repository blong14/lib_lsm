const std = @import("std");

const PageSize = std.mem.page_size;

pub const Opts = struct {
    data_dir: []const u8,
    sst_capacity: usize,
    wal_capacity: usize,
};

pub fn defaultOpts() Opts {
     return .{
        .data_dir = "data",
        // TODO: temp
        .sst_capacity = PageSize * PageSize / 2,
        .wal_capacity = PageSize * PageSize,
    };
}
