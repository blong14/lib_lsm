const std = @import("std");

const Build = std.Build;
const CompileStep = std.Build.Step.Compile;
const OptimizeMode = std.builtin.OptimizeMode;
const ResolvedTarget = std.Build.ResolvedTarget;

pub fn buildLsm(b: *Build, target: ResolvedTarget, optimize: OptimizeMode) *CompileStep {
    const exe = b.addExecutable(.{
        .name = "lsm",
        .root_source_file = b.path("test/lsm/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(exe);
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run-lsm", "Run the test app with no concurrency");
    run_step.dependOn(&run_cmd.step);
    return exe;
}

pub fn buildSystemV(b: *Build, target: ResolvedTarget, optimize: OptimizeMode) *CompileStep {
    const exe = b.addExecutable(.{
        .name = "systemv",
        .root_source_file = b.path("test/systemv/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(exe);
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run-stmv", "Run the test app with SystemV");
    run_step.dependOn(&run_cmd.step);
    return exe;
}
