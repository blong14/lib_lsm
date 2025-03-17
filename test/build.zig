const std = @import("std");

const Build = std.Build;
const CompileStep = std.Build.Step.Compile;
const OptimizeMode = std.builtin.OptimizeMode;
const ResolvedTarget = std.Build.ResolvedTarget;

pub fn buildLsm(b: *Build, target: ResolvedTarget, optimize: OptimizeMode) *CompileStep {
    const exe = b.addExecutable(.{
        .name = "xlsm",
        .root_source_file = b.path("test/lsm/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run-xlsm", "Run the test app with no concurrency");
    run_step.dependOn(&run_cmd.step);
    return exe;
}
