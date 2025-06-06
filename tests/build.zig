const std = @import("std");

const Build = std.Build;
const CompileStep = std.Build.Step.Compile;
const OptimizeMode = std.builtin.OptimizeMode;
const ResolvedTarget = std.Build.ResolvedTarget;

pub fn buildLsm(b: *Build, target: ResolvedTarget, optimize: OptimizeMode) *CompileStep {
    const exe = b.addExecutable(.{
        .name = "xlsm",
        .optimize = optimize,
        .root_source_file = b.path("tests/lsm/main.zig"),
        .target = target,
        // .sanitize_thread = true,
        .error_tracing = true,
        .unwind_tables = true,
    });
    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("xlsm", "Run the integration tests");
    run_step.dependOn(&run_cmd.step);
    return exe;
}
