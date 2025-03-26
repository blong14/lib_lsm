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

export fn lsm_init() ?*anyopaque {
    return defaultDatabase(std.heap.c_allocator) catch return null;
}

export fn lsm_read(addr: *anyopaque, key: [*c]const u8) [*c]const u8 {
    const db: *Database = @ptrCast(@alignCast(addr));
    const k = std.mem.span(key);
    const kv = db.read(k) catch {
        return null;
    };
    return &kv.value[0];
}

export fn lsm_write(addr: *anyopaque, key: [*c]const u8, value: [*c]const u8) bool {
    const db: *Database = @ptrCast(@alignCast(addr));
    const k = std.mem.span(key);
    const v = std.mem.span(value);
    db.write(k, v) catch {
        return false;
    };
    return true;
}
