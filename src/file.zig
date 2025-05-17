const std = @import("std");

const File = std.fs.File;

pub fn open(path: []const u8) !File {
    return std.fs.cwd().createFile(path, .{ .read = true, .truncate = false });
}

pub fn openWithCapacity(path: []const u8, capacity: usize) !File {
    var file = try open(path);
    try file.setEndPos(capacity);
    return file;
}

pub fn openAndTruncate(path: []const u8, capacity: usize) !File {
    const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
    try file.setEndPos(capacity);
    return file;
}

pub fn delete(path: []const u8) !void {
    std.log.debug("deleting file {s}", .{path});
    return std.fs.cwd().deleteFile(path);
}
