const std = @import("std");
const cmds = @import("test/build.zig");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // Add lib specific deps
    const msgpack = b.dependency("zig-msgpack", .{
        .target = target,
        .optimize = optimize,
    });
    const fast_csv = b.dependency("csv-fast-reader", .{
        .target = target,
        .optimize = optimize,
    });

    // Add custom modules so they can be referenced from our test directory
    const lsm = b.addModule("lsm", .{ .root_source_file = b.path("src/main.zig") });
    lsm.addImport("msgpack", msgpack.module("msgpack"));
    lsm.addIncludePath(fast_csv.path(""));

    // Main library build definition
    {
        const lib = b.addStaticLibrary(.{
            .name = "lib_lsm",
            // In this case the main source file is merely a path, however, in more
            // complicated build scripts, this could be a generated file.
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        });

        lib.addCSourceFiles(.{
            .root = fast_csv.path(""),
            .files = &.{"csv.c"},
        });
        lib.installHeadersDirectory(fast_csv.path(""), "", .{
            .include_extensions = &.{"csv.h"},
        });

        lib.root_module.addImport("msgpack", msgpack.module("msgpack"));
        lib.addIncludePath(fast_csv.path(""));
        lib.linkLibC();

        // This declares intent for the library to be installed into the standard
        // location when the user invokes the "install" step (the default step when
        // running `zig build`).
        b.installArtifact(lib);

        // Creates a step for unit testing. This only builds the test executable
        // but does not run it.
        const main_tests = b.addTest(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        });
        main_tests.root_module.addImport("msgpack", msgpack.module("msgpack"));

        main_tests.linkLibC();
        const run_main_tests = b.addRunArtifact(main_tests);

        // This creates a build step. It will be visible in the `zig build --help` menu,
        // and can be selected like this: `zig build test`
        // This will evaluate the `test` step rather than the default, which is "install".
        const test_step = b.step("test", "Run library tests");
        test_step.dependOn(&run_main_tests.step);
    }

    // Integration tests
    {
        const exe = cmds.buildLsm(b, target, optimize);
        const clap = b.dependency("clap", .{});
        exe.addCSourceFiles(.{
            .root = fast_csv.path(""),
            .files = &.{"csv.c"},
        });
        exe.installHeadersDirectory(fast_csv.path(""), "", .{
            .include_extensions = &.{"csv.h"},
        });
        exe.root_module.addImport("clap", clap.module("clap"));
        exe.root_module.addImport("msgpack", msgpack.module("msgpack"));
        exe.root_module.addImport("lsm", lsm);
        exe.linkLibC();

        b.installArtifact(exe);
    }
}
