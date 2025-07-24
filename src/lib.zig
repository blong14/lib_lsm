const std = @import("std");

const jemalloc = @import("jemalloc");

const iter = @import("iterator.zig");
const opt = @import("opts.zig");
const spv = @import("supervisor.zig");

const Allocator = std.mem.Allocator;

const DatabaseSupervisor = spv.DatabaseSupervisor;
const Iterator = iter.Iterator;

const allocator = jemalloc.allocator;

var supervisor: *DatabaseSupervisor = undefined;

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

    try db.open(allocator);

    supervisor = try DatabaseSupervisor.init(
        allocator,
        db,
        .{},
    );

    try supervisor.start();

    return db;
}

pub fn deinit(db: *Database) void {
    supervisor.stop();
    supervisor.deinit();

    db.deinit(allocator);
    allocator.destroy(db);
}

pub fn read(db: *Database, key: []const u8) !?KV {
    return try db.read(key);
}

pub fn write(db: *Database, kv: KV) !void {
    try db.write(kv);
    supervisor.submitEvent(.{ .write_completed = .{ .bytes = kv.len() } });
}

// Public C Interface

export fn lsm_init() ?*anyopaque {
    const opts = defaultOpts();
    return init(allocator, opts) catch return null;
}

export fn lsm_read(addr: *anyopaque, k: [*c]const u8) [*c]const u8 {
    const db: *Database = @ptrCast(@alignCast(addr));
    const key = std.mem.span(k);

    if (read(db, key) catch return null) |kv| {
        return &kv.value[0];
    }

    return null;
}

export fn lsm_write(addr: *anyopaque, k: [*c]const u8, v: [*c]const u8) bool {
    const db: *Database = @ptrCast(@alignCast(addr));
    const key = std.mem.span(k);
    const value = std.mem.span(v);

    var kv = KV.initOwned(allocator, key, value) catch return false;
    defer kv.deinit(allocator);

    write(db, kv) catch return false;

    return true;
}

export fn lsm_scan(addr: *anyopaque, start_key: [*c]const u8, end_key: [*c]const u8) ?*anyopaque {
    const db: *Database = @ptrCast(@alignCast(addr));
    const start = std.mem.span(start_key);
    const end = std.mem.span(end_key);

    const it = allocator.create(Iterator(KV)) catch return null;
    it.* = db.scan(allocator, start, end) catch return null;
    return it;
}

export fn lsm_iter_next(addr: *anyopaque) [*c]const u8 {
    const it: *Iterator(KV) = @ptrCast(@alignCast(addr));
    if (it.next()) |nxt| return &nxt.value[0];
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

    deinit(db);

    return true;
}
