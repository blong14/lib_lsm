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