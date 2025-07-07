const std = @import("std");

const atomic = std.atomic;
const debug = std.debug;
const heap = std.heap;
const io = std.io;
const math = std.math;
const mem = std.mem;
const testing = std.testing;
const hasher = std.hash.Murmur2_64;

const Allocator = mem.Allocator;
const ArenaAllocator = heap.ArenaAllocator;
const AtomicValue = atomic.Value;
const Mutex = std.Thread.Mutex;
const Order = math.Order;

const csv = @cImport({
    @cInclude("csv.h");
});
pub const CsvOpen2 = csv.CsvOpen2;
pub const CsvClose = csv.CsvClose;
pub const ReadNextRow = csv.CsvReadNextRow;
pub const ReadNextCol = csv.CsvReadNextCol;

const file = @import("file.zig");
pub const OpenFile = file.open;

const iter = @import("iterator.zig");
pub const Iterator = iter.Iterator;

const msgq = @import("msgqueue.zig");
pub const ProcessMessageQueue = msgq.ProcessMessageQueue;
pub const ThreadMessageQueue = msgq.Queue;

const mtbl = @import("memtable.zig");
pub const Memtable = mtbl.Memtable;

const opt = @import("opts.zig");
pub const Opts = opt.Opts;
pub const defaultOpts = opt.defaultOpts;
pub const withDataDirOpts = opt.withDataDirOpts;

const prof = @import("profile.zig");
pub const BeginProfile = prof.BeginProfile;
pub const EndProfile = prof.EndProfile;
pub const BlockProfiler = prof.BlockProfiler;

pub const Database = @import("database.zig").Database;
pub const ThreadSafeBumpAllocator = @import("bump_allocator.zig").ThreadSafeBumpAllocator;
pub const KV = @import("kv.zig").KV;

const SSTable = @import("sstable.zig").SSTable;
const TableMap = @import("tablemap.zig").TableMap;
const WAL = @import("wal.zig").WAL;

pub fn defaultDatabase(alloc: Allocator) !*Database {
    return try databaseFromOpts(alloc, defaultOpts());
}

pub fn databaseFromOpts(alloc: Allocator, opts: Opts) !*Database {
    return try Database.init(alloc, opts);
}

// Public C Interface
const jemalloc = @import("jemalloc");
const allocator = jemalloc.allocator;

export fn lsm_init() ?*anyopaque {
    const db = defaultDatabase(allocator) catch return null;
    db.open(allocator) catch return null;
    return db;
}

export fn lsm_read(addr: *anyopaque, key: [*c]const u8) [*c]const u8 {
    const db: *Database = @ptrCast(@alignCast(addr));
    const k = std.mem.span(key);
    const kv = db.read(k) catch return null;
    return &kv.?.value[0];
}

export fn lsm_value_deinit(value: [*c]const u8) bool {
    const v = std.mem.span(value);
    allocator.free(v);
    return true;
}

export fn lsm_write(addr: *anyopaque, key: [*c]const u8, value: [*c]const u8) bool {
    const db: *Database = @ptrCast(@alignCast(addr));
    const k = std.mem.span(key);
    const v = std.mem.span(value);

    var kv = KV.initOwned(allocator, k, v) catch return false;
    defer kv.deinit(allocator);

    db.xwrite(allocator, kv) catch return false;

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
    db.deinit(allocator);
    allocator.destroy(db);
    return true;
}
