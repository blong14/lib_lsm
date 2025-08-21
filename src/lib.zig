const std = @import("std");

const jemalloc = @import("jemalloc");

const iter = @import("iterator.zig");
const opt = @import("opts.zig");
const spv = @import("supervisor.zig");

const Allocator = std.mem.Allocator;

const DatabaseSupervisor = spv.DatabaseSupervisor;
const Iterator = iter.Iterator;
const WAL = @import("wal.zig").WAL;

const allocator = jemalloc.allocator;

var supervisor: *DatabaseSupervisor = undefined;

var wal: *WAL = undefined;

// Public Interface

pub const Opts = opt.Opts;
pub const Database = @import("database.zig").Database;
pub const KV = @import("kv.zig").KV;

pub const defaultOpts = opt.defaultOpts;
pub const withDataDirOpts = opt.withDataDirOpts;

pub fn defaultDatabase(alloc: Allocator) !*Database {
    return try databaseFromOpts(alloc, defaultOpts());
}

pub fn databaseFromOpts(alloc: Allocator, opts: Opts) !*Database {
    return try Database.init(alloc, opts);
}

pub fn init(alloc: Allocator, opts: Opts) !*Database {
    const db = try databaseFromOpts(alloc, opts);

    try db.open(alloc);

    supervisor = try DatabaseSupervisor.init(
        alloc,
        db,
        .{},
    );
    try supervisor.start();

    // wal = try alloc.create(WAL);
    // wal.* = try WAL.init(alloc, .{ .Dir = opts.data_dir });

    return db;
}

pub fn deinit(alloc: Allocator, db: *Database) void {
    supervisor.stop();
    supervisor.deinit();

    // wal.deinit(alloc) catch unreachable;
    // alloc.destroy(wal);

    db.deinit(alloc);
    alloc.destroy(db);
}

pub fn read(db: *Database, key: []const u8) !?KV {
    return try db.read(key);
}

pub fn write(db: *Database, kv: KV) !void {
    // try wal.write(allocator, kv);
    try db.write(kv);
    supervisor.submitEvent(.{ .write_completed = .{ .bytes = kv.len() } });
}

// Public C Interface

export fn lsm_init() ?*anyopaque {
    const opts = defaultOpts();
    return init(allocator, opts) catch return null;
}

export fn lsm_init_with_config(addr: *anyopaque) ?*anyopaque {
    const opts: *Opts = @ptrCast(@alignCast(addr));
    return init(allocator, opts.*) catch return null;
}

export fn lsm_read(addr: *anyopaque, k: [*c]const u8) [*c]const u8 {
    if (k == null) return null;

    const db: *Database = @ptrCast(@alignCast(addr));
    const key = std.mem.span(k);

    if (key.len == 0) return null;

    if (read(db, key) catch return null) |kv| {
        if (kv.value.len == 0) return null;
        return &kv.value[0];
    }

    return null;
}

export fn lsm_write(addr: *anyopaque, k: [*c]const u8, v: [*c]const u8) bool {
    if (k == null or v == null) return false;

    const key = std.mem.span(k);
    const value = std.mem.span(v);

    var kv = KV.initOwned(allocator, key, value) catch return false;
    defer kv.deinit(allocator);

    const db: *Database = @ptrCast(@alignCast(addr));

    write(db, kv) catch return false;

    return true;
}

export fn lsm_scan(addr: *anyopaque, start_key: [*c]const u8, end_key: [*c]const u8) ?*anyopaque {
    if (start_key == null or end_key == null) return null;

    const db: *Database = @ptrCast(@alignCast(addr));
    const start = std.mem.span(start_key);
    const end = std.mem.span(end_key);

    if (start.len == 0 or end.len == 0) return null;

    const it = allocator.create(Iterator(KV)) catch return null;
    it.* = db.scan(allocator, start, end) catch {
        allocator.destroy(it);
        return null;
    };
    return it;
}

export fn lsm_iter_next(addr: *anyopaque) [*c]const u8 {
    const it: *Iterator(KV) = @ptrCast(@alignCast(addr));
    if (it.next()) |nxt| {
        if (nxt.value.len == 0) return null;
        return &nxt.value[0];
    }
    return null;
}

export fn lsm_iter_deinit(addr: *anyopaque) bool {
    const it: *Iterator(KV) = @ptrCast(@alignCast(addr));
    it.deinit();
    allocator.destroy(it);
    return true;
}

export fn lsm_deinit(addr: *anyopaque) bool {
    const db: *Database = @ptrCast(@alignCast(addr));

    deinit(allocator, db);

    return true;
}

test "C interface lsm_init_with_config" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = std.testing.tmpDir(.{});
    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    var opts = withDataDirOpts(dir_name);
    const db = lsm_init_with_config(&opts);

    const actual: *Database = @ptrCast(@alignCast(db));

    try std.testing.expect(actual.capacity > 0);
    try std.testing.expectEqualStrings(dir_name, actual.opts.data_dir);
    try std.testing.expect(lsm_deinit(db.?));
}

test "C interface lsm_write with invalid data returns false" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = std.testing.tmpDir(.{});
    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const opts = withDataDirOpts(dir_name);
    const db = try Database.init(alloc, opts);
    defer {
        db.deinit(alloc);
        alloc.destroy(db);
    }

    const result = lsm_write(db, null, "value");
    try std.testing.expect(!result);
}

test "C interface lsm_read with non-existent key returns null" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = std.testing.tmpDir(.{});
    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const opts = withDataDirOpts(dir_name);
    const db = try Database.init(alloc, opts);
    defer {
        db.deinit(alloc);
        alloc.destroy(db);
    }

    const result = lsm_read(db, "non_existent_key");
    try std.testing.expect(result == null);
}

test "C interface lsm_scan" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    const testDir = std.testing.tmpDir(.{});
    const dir_name = try testDir.dir.realpathAlloc(alloc, ".");
    defer testDir.dir.deleteTree(dir_name) catch {};

    const opts = withDataDirOpts(dir_name);
    const db = try Database.init(alloc, opts);
    defer {
        db.deinit(alloc);
        alloc.destroy(db);
    }

    const keys = [_]KV{
        try KV.initOwned(alloc, "__key_c__", "__key_c__"),
        try KV.initOwned(alloc, "__key_b__", "__key_b__"),
        try KV.initOwned(alloc, "__key_a__", "__key_a__"),
    };
    for (keys) |expected| {
        try db.write(expected);
    }

    const scanner = lsm_scan(db, "__key_a__", "__key_c__");

    try std.testing.expectEqualStrings(
        "__key_a__",
        std.mem.span(lsm_iter_next(scanner.?)),
    );
    try std.testing.expectEqualStrings(
        "__key_b__",
        std.mem.span(lsm_iter_next(scanner.?)),
    );
    try std.testing.expectEqualStrings(
        "__key_c__",
        std.mem.span(lsm_iter_next(scanner.?)),
    );
    try std.testing.expectEqual(null, lsm_iter_next(scanner.?));
    try std.testing.expect(lsm_iter_deinit(scanner.?));
}
