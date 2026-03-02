import { describe, expect, it } from "vitest";
import {
  buildErrorSignature,
  buildIdempotencyKey,
  calculateBaselineMetrics,
  computeCollectionWindow,
  computeNearMissFlag,
  evaluateGuardrails,
  evaluateGuardrailsWithFlags,
  reconcileRunState,
  type OpsFeatureFlags,
} from "@/lib/ops-control-plane";

describe("ops-control-plane", () => {
  it("builds stable signature for semantically equivalent errors", () => {
    const sig1 = buildErrorSignature({
      errorCode: "SCHEMA_MISSING",
      errorMessage: "Column id_123 not found at 2026-03-01T01:01:01Z",
      stackTopFrames: ["fileA:99", "fileB:123"],
    });
    const sig2 = buildErrorSignature({
      errorCode: "SCHEMA_MISSING",
      errorMessage: "Column id_456 not found at 2026-03-01T01:02:00Z",
      stackTopFrames: ["fileA:88", "fileB:456"],
    });
    expect(sig1).toEqual(sig2);
  });

  it("reconciles running portal state when dbx already terminated", () => {
    const result = reconcileRunState("TERMINATED", "RUNNING");
    expect(result.runStatus).toBe("SUCCEEDED");
    expect(result.inconsistent).toBe(true);
  });

  it("promotes succeeded run to SUCCEEDED_WITH_ISSUES when guardrails fail", () => {
    const result = evaluateGuardrails(
      {
        runId: "run_1",
        status: "SUCCEEDED",
        recordsWritten: 0,
        dqFailCount: 1,
        watermarkBefore: "2026-03-01",
        watermarkAfter: "2026-03-01",
      },
      {
        portalJobId: "job_1",
        expectedFrequency: "daily",
        watermarkRequired: true,
        dqRequired: true,
        silentFailureCheck: true,
        volumeMin: 1,
        volumeMax: 1000,
      }
    );
    expect(result.finalStatus).toBe("SUCCEEDED_WITH_ISSUES");
    expect(result.needsAttention).toBe(true);
    expect(result.checks.filter((c) => c.status === "FAIL").length).toBeGreaterThan(0);
  });

  it("calculates baseline percentiles from valid durations", () => {
    const b = calculateBaselineMetrics([1000, 2000, 3000, 4000, 5000]);
    expect(b.p50DurationMs).toBe(3000);
    expect(b.p95DurationMs).toBe(5000);
    expect(b.sampleSize).toBe(5);
  });

  it("flags near miss only with sufficient sample and duration above threshold", () => {
    const baseline = calculateBaselineMetrics([100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200]);
    expect(computeNearMissFlag(300, baseline, 10)).toBe(true);
    expect(computeNearMissFlag(180, baseline, 10)).toBe(false);
  });

  it("evaluateGuardrailsWithFlags in monitor mode keeps original status", () => {
    const telemetry = {
      runId: "run_ff",
      status: "SUCCEEDED" as const,
      recordsWritten: 0,
      watermarkBefore: "2026-03-01",
      watermarkAfter: "2026-03-01",
    };
    const guardrail = {
      portalJobId: "job_ff",
      expectedFrequency: "daily" as const,
      watermarkRequired: true,
      dqRequired: false,
      silentFailureCheck: true,
    };
    const monitorFlags: OpsFeatureFlags = {
      enableGuardrails: true,
      enableReplayPolicy: true,
      enableNearMiss: true,
      enableIncidentAutoCreate: false,
      guardrailMode: "monitor",
    };
    const result = evaluateGuardrailsWithFlags(telemetry, guardrail, monitorFlags);
    expect(result.finalStatus).toBe("SUCCEEDED");
    expect(result.checks.length).toBeGreaterThan(0);
    expect(result.needsAttention).toBe(true);
  });

  it("evaluateGuardrailsWithFlags in enforce mode promotes to SUCCEEDED_WITH_ISSUES", () => {
    const telemetry = {
      runId: "run_ff2",
      status: "SUCCEEDED" as const,
      recordsWritten: 0,
      watermarkBefore: "2026-03-01",
      watermarkAfter: "2026-03-01",
    };
    const guardrail = {
      portalJobId: "job_ff2",
      expectedFrequency: "daily" as const,
      watermarkRequired: true,
      dqRequired: false,
      silentFailureCheck: true,
    };
    const enforceFlags: OpsFeatureFlags = {
      enableGuardrails: true,
      enableReplayPolicy: true,
      enableNearMiss: true,
      enableIncidentAutoCreate: false,
      guardrailMode: "enforce",
    };
    const result = evaluateGuardrailsWithFlags(telemetry, guardrail, enforceFlags);
    expect(result.finalStatus).toBe("SUCCEEDED_WITH_ISSUES");
  });

  it("evaluateGuardrailsWithFlags with guardrails disabled returns no checks", () => {
    const telemetry = { runId: "run_ff3", status: "SUCCEEDED" as const };
    const guardrail = {
      portalJobId: "job_ff3",
      expectedFrequency: "daily" as const,
      watermarkRequired: true,
      dqRequired: true,
      silentFailureCheck: true,
    };
    const disabledFlags: OpsFeatureFlags = {
      enableGuardrails: false,
      enableReplayPolicy: false,
      enableNearMiss: false,
      enableIncidentAutoCreate: false,
      guardrailMode: "enforce",
    };
    const result = evaluateGuardrailsWithFlags(telemetry, guardrail, disabledFlags);
    expect(result.checks).toHaveLength(0);
    expect(result.needsAttention).toBe(false);
    expect(result.finalStatus).toBe("SUCCEEDED");
  });

  it("builds deterministic idempotency key", () => {
    const key = buildIdempotencyKey({ workspaceId: "ws1", jobId: "j1", runId: "r1", attempt: 2 });
    expect(key).toBe("ws1|j1|r1|2");
  });

  it("computes collection window with overlap", () => {
    const base = new Date("2026-03-01T12:00:00Z");
    const window = computeCollectionWindow({ lastCollectedAt: base, overlapMinutes: 15 });
    expect(window.start.getTime()).toBe(base.getTime() - 15 * 60_000);
    expect(window.end.getTime()).toBeGreaterThan(base.getTime());
  });
});

