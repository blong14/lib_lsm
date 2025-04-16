const std = @import("std");
const cmds = @import("tests/build.zig");

pub fn zig_fmt(b: *std.Build) *std.Build.Step.Run {
    const cmd = b.addSystemCommand(
        &[_][]const u8{
            "zig",
            "fmt",
            "src",
        },
    );
    return cmd;
}

pub fn cp_lsm_headers(b: *std.Build) *std.Build.Step.Run {
    const cmd = b.addSystemCommand(
        &[_][]const u8{
            "cp",
            "include/lib_lsm.h",
            "zig-out/include/lib_lsm.h",
        },
    );
    return cmd;
}

pub fn go_fmt(b: *std.Build) *std.Build.Step.Run {
    const cmd = b.addSystemCommand(
        &[_][]const u8{
            "go",
            "fmt",
            "./...",
        },
    );
    return cmd;
}

pub fn go_build(b: *std.Build) *std.Build.Step.Run {
    const cmd = b.addSystemCommand(
        &[_][]const u8{
            "go",
            "build",
            "-o=zig-out/bin/gopg",
            "src/main.go",
        },
    );
    return cmd;
}

pub fn cbindgen_build(b: *std.Build) *std.Build.Step.Run {
    const cmd = b.addSystemCommand(
        &[_][]const u8{
            "cbindgen",
            "--config",
            "cbindgen.toml",
            "--crate",
            "concurrent-skiplist",
            "--output",
            "zig-out/include/skiplist.h",
        },
    );
    return cmd;
}

pub fn rust_build(b: *std.Build) *std.Build.Step.Run {
    // https://nnethercote.github.io/perf-book/build-configuration.html
    const rust_headers = cbindgen_build(b);
    const cmd = b.addSystemCommand(
        &[_][]const u8{
            // "MALLOC_CONF='thp:always,metadata_thp:always'",
            "cargo",
            "build",
            "--release",
            "--target-dir=zig-out/lib",
        },
    );
    cmd.step.dependOn(&rust_headers.step);
    return cmd;
}

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

    // utilitiy commands
    const zigfmt = zig_fmt(b);
    const gofmt = go_fmt(b);
    
    const fmt_step = b.step("fmt", "Format source files");
    fmt_step.dependOn(&zigfmt.step);
    fmt_step.dependOn(&gofmt.step);
    
    // Add lib specific deps (check zig cacheing)
    // TODO: Add rust dependency install
    // TODO: Add go dependency install
    // TODO: Add zig dependency install
    const clap = b.dependency("clap", .{
        .target = target,
        .optimize = optimize,
    });
    const fast_csv = b.dependency("csv-fast-reader", .{
        .target = target,
        .optimize = optimize,
    });
    const jemalloc = b.dependency("jemalloc", .{
        .target = target,
        .optimize = optimize,
        .link_vendor = false,
    });

    // make zig-out/include/skiplist.h
    // make zig-out/lib/libconcurrent_skiplist.so
    const rust = rust_build(b);
    const rust_make_step = b.step("rust", "Build the shared rust library");
    rust_make_step.dependOn(&rust.step);
    
    // cp include/lib_lsm.h zig-out/include
    const lsm_headers = cp_lsm_headers(b);
    lsm_headers.step.dependOn(&rust.step);

    // make zig-out/lib/liblib_lsm.a
    const lib = b.addStaticLibrary(.{
        .name = "lib_lsm",
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.step.dependOn(&zigfmt.step);
    lib.step.dependOn(&rust.step);
    lib.step.dependOn(&lsm_headers.step);
    lib.root_module.addImport("jemalloc", jemalloc.module("jemalloc"));
    lib.addIncludePath(fast_csv.path(""));
    lib.addCSourceFiles(.{ .root = fast_csv.path(""), .files = &.{"csv.c"} });
    lib.addIncludePath(b.path("zig-out/include"));
    lib.addObjectFile(b.path("zig-out/lib/release/libconcurrent_skiplist.so"));
    lib.linkSystemLibrary("jemalloc");
    lib.linkLibC();

    // This declares intent for the library to be installed into the standard
    // location when the user invokes the "install" step (the default step when
    // running `zig build`)
    b.installArtifact(lib);

    // Main module to be imported into separate run artifacts
    const lsm = b.addModule("lsm", .{ .root_source_file = b.path("src/lib.zig") });
    lsm.addImport("jemalloc", jemalloc.module("jemalloc"));
    lsm.addIncludePath(fast_csv.path(""));
    lsm.addCSourceFiles(.{ .root = fast_csv.path(""), .files = &.{"csv.c"} });
    lsm.addIncludePath(b.path("zig-out/include"));
    lsm.addObjectFile(b.path("zig-out/lib/release/libconcurrent_skiplist.so"));

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const main_tests = b.addTest(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    main_tests.step.dependOn(&rust.step);
    main_tests.step.dependOn(&lsm_headers.step);
    main_tests.root_module.addImport("lsm", lsm);
    main_tests.root_module.addImport("jemalloc", jemalloc.module("jemalloc"));
    main_tests.addIncludePath(b.path("zig-out/include"));
    main_tests.linkSystemLibrary("jemalloc");
    main_tests.linkLibC();

    const run_main_tests = b.addRunArtifact(main_tests);
    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);

    // make build-integration-tests
    const lsmx = cmds.buildLsm(b, target, optimize);
    lsmx.step.dependOn(&rust.step);
    lsmx.step.dependOn(&lsm_headers.step);
    lsmx.root_module.addImport("lsm", lsm);
    lsmx.root_module.addImport("clap", clap.module("clap"));
    lsmx.root_module.addImport("jemalloc", jemalloc.module("jemalloc"));
    lsmx.linkSystemLibrary("jemalloc");
    lsmx.linkLibC();

    // make build-lsmctl
    const exe = b.addExecutable(.{
        .name = "lsmctl",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.step.dependOn(&rust.step);
    exe.step.dependOn(&lsm_headers.step);
    exe.root_module.addImport("lsm", lsm);
    exe.root_module.addImport("clap", clap.module("clap"));
    exe.root_module.addImport("jemalloc", jemalloc.module("jemalloc"));
    exe.linkSystemLibrary("jemalloc");
    exe.linkLibC();

    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("lsmctl", "Run the lsm cli");
    run_step.dependOn(&run_cmd.step);

    // make build gopg
    const go = go_build(b);
    go.step.dependOn(&gofmt.step);
    go.step.dependOn(b.getInstallStep());

    const go_make_step = b.step("go", "Build the go server");
    go_make_step.dependOn(&go.step);
}
