"""Unit tests for :mod:`workflow_monitor.htcondor_poll`.

These tests never touch a real HTCondor.  Subprocess-backed query functions are
exercised by:

  * monkeypatching the ``_try_*_bindings`` helpers to return ``None`` (so the
    binding path is skipped deterministically regardless of whether the
    ``htcondor`` modules happen to import on this machine), and
  * monkeypatching ``workflow_monitor.htcondor_poll.subprocess.run`` to return a
    fake ``CompletedProcess``-like object or to raise.

``CondorBackoff`` jitter (``random.uniform``) is monkeypatched to ``0.0`` for
deterministic delay schedules.
"""

from __future__ import annotations

import json
import subprocess
import types

import pytest

from workflow_monitor import htcondor_poll as hp
from workflow_monitor.htcondor_poll import (
    JOB_STATUS,
    CondorBackoff,
    CondorQueryError,
    PoolSummary,
    cpu_efficiency,
    format_disk_usage,
    format_efficiency,
    format_job_status,
    format_resources,
    format_transfer,
    memory_efficiency,
    query_history,
    query_queue,
    query_slots,
    queue_wait_seconds,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def fake_proc(returncode: int = 0, stdout: str = "", stderr: str = ""):
    """A minimal CompletedProcess-like object for monkeypatching subprocess.run."""
    return types.SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


@pytest.fixture
def no_bindings(monkeypatch):
    """Force every query function down its subprocess fallback path."""
    monkeypatch.setattr(hp, "_try_python_bindings", lambda *a, **k: None)
    monkeypatch.setattr(hp, "_try_history_bindings", lambda *a, **k: None)
    monkeypatch.setattr(hp, "_try_slots_bindings", lambda *a, **k: None)


@pytest.fixture
def no_jitter(monkeypatch):
    """Make CondorBackoff jitter deterministic (always 0)."""
    monkeypatch.setattr(hp.random, "uniform", lambda a, b: 0.0)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Constants & format_job_status
# ─────────────────────────────────────────────────────────────────────────────


def test_job_status_mapping():
    assert JOB_STATUS == {
        1: "Idle",
        2: "Running",
        3: "Removed",
        4: "Completed",
        5: "Held",
        6: "TransferOutput",
        7: "Suspended",
    }


def test_format_job_status_known():
    assert format_job_status(2) == "Running"
    assert format_job_status(5) == "Held"


def test_format_job_status_numeric_string_known():
    # int("2") == 2 → mapped
    assert format_job_status("2") == "Running"


def test_format_job_status_unknown_int_passthrough():
    assert format_job_status(99) == "99"


def test_format_job_status_non_int():
    assert format_job_status("weird") == "weird"
    assert format_job_status(None) == "None"


# ─────────────────────────────────────────────────────────────────────────────
# 2. CondorBackoff
# ─────────────────────────────────────────────────────────────────────────────


def test_backoff_init_clamps_base():
    b = CondorBackoff(0.0)
    assert b._base == 0.1
    b2 = CondorBackoff(-5.0)
    assert b2._base == 0.1
    b3 = CondorBackoff(2.5)
    assert b3._base == 2.5


def test_backoff_init_defaults():
    b = CondorBackoff(1.0)
    assert b._max == 60.0
    assert b.fail_streak == 0
    assert b._retry_at == 0.0


def test_backoff_due_when_no_failures():
    b = CondorBackoff(1.0)
    assert b.due(0.0) is True
    assert b.due(123456.0) is True


def test_backoff_due_after_failure(no_jitter):
    b = CondorBackoff(1.0, max_backoff=60.0)
    b.record(False, now=100.0)  # streak1 → delay 2 → retry_at 102
    assert b.due(101.0) is False
    assert b.due(102.0) is True
    assert b.due(103.0) is True


def test_backoff_record_failure_delay_schedule(no_jitter):
    # base=1, max=60: 2,4,8,16,32,60(capped),60...
    b = CondorBackoff(1.0, max_backoff=60.0)
    now = 0.0
    expected = [2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0]
    for streak, exp_delay in enumerate(expected, start=1):
        b.record(False, now=now)
        assert b.fail_streak == streak
        assert b._retry_at == pytest.approx(now + exp_delay), (
            f"streak {streak} delay {exp_delay}"
        )


def test_backoff_delay_cap_at_min_exponent(no_jitter):
    # min(streak, 10) caps the exponent so it never overflows beyond 2**10.
    b = CondorBackoff(0.1, max_backoff=1000.0)
    for _ in range(15):
        b.record(False, now=0.0)
    # streak now 15 → exponent capped at 10 → 0.1 * 1024 = 102.4
    assert b._retry_at == pytest.approx(102.4)


def test_backoff_record_success_resets_and_recovered_flag(no_jitter):
    b = CondorBackoff(1.0)
    # success with no prior failures → not a recovery
    assert b.record(True, now=0.0) is False
    # build up failures
    b.record(False, now=0.0)
    b.record(False, now=0.0)
    assert b.fail_streak == 2
    # first success after failures → recovered True, state reset
    assert b.record(True, now=10.0) is True
    assert b.fail_streak == 0
    assert b._retry_at == 0.0
    # subsequent success → not a recovery
    assert b.record(True, now=20.0) is False


def test_backoff_record_failure_returns_false(no_jitter):
    b = CondorBackoff(1.0)
    assert b.record(False, now=0.0) is False


def test_backoff_next_retry_in(no_jitter):
    b = CondorBackoff(1.0, max_backoff=60.0)
    assert b.next_retry_in(0.0) == 0.0  # streak 0
    b.record(False, now=100.0)  # delay 2 → retry_at 102
    assert b.next_retry_in(100.0) == pytest.approx(2.0)
    assert b.next_retry_in(101.5) == pytest.approx(0.5)
    # past retry_at → clamps to 0
    assert b.next_retry_in(105.0) == 0.0


def test_backoff_jitter_adds_within_bounds(monkeypatch):
    # With max jitter, delay stays >= base schedule and within +10% (cap 5s).
    monkeypatch.setattr(hp.random, "uniform", lambda a, b: b)
    bo = CondorBackoff(1.0, max_backoff=60.0)
    bo.record(False, now=0.0)  # base delay 2, jitter min(0.2, 5)=0.2
    assert bo._retry_at == pytest.approx(2.2)


# ─────────────────────────────────────────────────────────────────────────────
# 3. format_resources
# ─────────────────────────────────────────────────────────────────────────────


def test_format_resources_cpus_and_memory_gb():
    assert format_resources({"RequestCpus": 2, "RequestMemory": 4096}) == "2c/4.0G"


def test_format_resources_memory_mb_under_1g():
    assert format_resources({"RequestCpus": 1, "RequestMemory": 512}) == "1c/512M"


def test_format_resources_with_gpus():
    out = format_resources({"RequestCpus": 1, "RequestMemory": 2048, "RequestGpus": 1})
    assert out == "1c/2.0G/1gpu"


def test_format_resources_zero_gpus_omitted():
    out = format_resources({"RequestCpus": 1, "RequestMemory": 1024, "RequestGpus": 0})
    assert out == "1c/1.0G"


def test_format_resources_memory_exactly_1024_is_gb():
    assert format_resources({"RequestMemory": 1024}) == "1.0G"


def test_format_resources_empty():
    assert format_resources({}) == ""


def test_format_resources_cpus_only():
    assert format_resources({"RequestCpus": 8}) == "8c"


# ─────────────────────────────────────────────────────────────────────────────
# 4. format_transfer
# ─────────────────────────────────────────────────────────────────────────────


def test_format_transfer_zero():
    assert format_transfer({}) == ""
    assert format_transfer({"BytesSent": 0, "BytesRecvd": 0}) == ""


def test_format_transfer_none_values_treated_as_zero():
    assert format_transfer({"BytesSent": None, "BytesRecvd": None}) == ""


def test_format_transfer_bytes():
    assert format_transfer({"BytesSent": 100, "BytesRecvd": 23}) == "123B"


def test_format_transfer_kilobytes():
    # 1024+1024 = 2048 → 2.0K
    assert format_transfer({"BytesSent": 1024, "BytesRecvd": 1024}) == "2.0K"


def test_format_transfer_megabytes():
    total = 5 * 1024 * 1024
    assert format_transfer({"BytesSent": total, "BytesRecvd": 0}) == "5.0M"


def test_format_transfer_gigabytes():
    total = 2 * 1024 * 1024 * 1024
    assert format_transfer({"BytesSent": total, "BytesRecvd": 0}) == "2.00G"


# ─────────────────────────────────────────────────────────────────────────────
# 5. queue_wait_seconds
# ─────────────────────────────────────────────────────────────────────────────


def test_queue_wait_seconds_normal():
    assert queue_wait_seconds({"QDate": 100, "JobStartDate": 110}) == 10.0


def test_queue_wait_seconds_negative_clamped():
    assert queue_wait_seconds({"QDate": 200, "JobStartDate": 100}) == 0.0


def test_queue_wait_seconds_missing_start():
    assert queue_wait_seconds({"QDate": 100}) is None


def test_queue_wait_seconds_missing_qdate():
    assert queue_wait_seconds({"JobStartDate": 110}) is None


def test_queue_wait_seconds_both_missing():
    assert queue_wait_seconds({}) is None


# ─────────────────────────────────────────────────────────────────────────────
# 6. cpu_efficiency
# ─────────────────────────────────────────────────────────────────────────────


def test_cpu_efficiency_basic():
    # 38 / (40 * 1) = 0.95
    eff = cpu_efficiency(
        {"RemoteWallClockTime": 40.0, "RemoteUserCpu": 38.0, "RequestCpus": 1}
    )
    assert eff == pytest.approx(0.95)


def test_cpu_efficiency_multi_cpu():
    # 30 / (60 * 2) = 0.25
    eff = cpu_efficiency(
        {"RemoteWallClockTime": 60.0, "RemoteUserCpu": 30.0, "RequestCpus": 2}
    )
    assert eff == pytest.approx(0.25)


def test_cpu_efficiency_default_request_cpus():
    # RequestCpus missing → defaults to 1
    eff = cpu_efficiency({"RemoteWallClockTime": 10.0, "RemoteUserCpu": 5.0})
    assert eff == pytest.approx(0.5)


def test_cpu_efficiency_missing_wall():
    assert cpu_efficiency({"RemoteUserCpu": 5.0}) is None


def test_cpu_efficiency_missing_user_cpu():
    assert cpu_efficiency({"RemoteWallClockTime": 10.0}) is None


def test_cpu_efficiency_zero_wall():
    assert cpu_efficiency({"RemoteWallClockTime": 0.0, "RemoteUserCpu": 5.0}) is None


def test_cpu_efficiency_zero_cpus():
    # Regression guard (fixed): a present RequestCpus == 0 must reach the
    # `cpus_f <= 0` guard rather than being rewritten to 1.0, so it returns None.
    assert (
        cpu_efficiency(
            {"RemoteWallClockTime": 10.0, "RemoteUserCpu": 5.0, "RequestCpus": 0}
        )
        is None
    )


def test_cpu_efficiency_negative_cpus_hits_guard():
    # A negative RequestCpus is truthy, so it survives the `if cpus else 1.0`
    # rewrite and is correctly rejected by the `cpus_f <= 0` guard.
    assert (
        cpu_efficiency(
            {"RemoteWallClockTime": 10.0, "RemoteUserCpu": 5.0, "RequestCpus": -2}
        )
        is None
    )


def test_cpu_efficiency_bad_types():
    assert (
        cpu_efficiency(
            {
                "RemoteWallClockTime": "abc",
                "RemoteUserCpu": 5.0,
                "RequestCpus": 1,
            }
        )
        is None
    )


# ─────────────────────────────────────────────────────────────────────────────
# 7. memory_efficiency
# ─────────────────────────────────────────────────────────────────────────────


def test_memory_efficiency_basic():
    # ImageSize 1048576 KB / (2048 MB * 1024) = 1048576 / 2097152 = 0.5
    eff = memory_efficiency({"ImageSize": 1048576, "RequestMemory": 2048})
    assert eff == pytest.approx(0.5)


def test_memory_efficiency_missing_image():
    assert memory_efficiency({"RequestMemory": 2048}) is None


def test_memory_efficiency_missing_request():
    assert memory_efficiency({"ImageSize": 1024}) is None


def test_memory_efficiency_zero_request():
    assert memory_efficiency({"ImageSize": 1024, "RequestMemory": 0}) is None


def test_memory_efficiency_bad_types():
    assert memory_efficiency({"ImageSize": "huge", "RequestMemory": 2048}) is None


# ─────────────────────────────────────────────────────────────────────────────
# 8. format_efficiency
# ─────────────────────────────────────────────────────────────────────────────


def test_format_efficiency_none():
    assert format_efficiency(None) == "-"


def test_format_efficiency_fraction():
    assert format_efficiency(0.85) == "85%"


def test_format_efficiency_rounds():
    assert format_efficiency(0.857) == "86%"
    assert format_efficiency(1.0) == "100%"
    assert format_efficiency(0.0) == "0%"


# ─────────────────────────────────────────────────────────────────────────────
# 9. format_disk_usage
# ─────────────────────────────────────────────────────────────────────────────


def test_format_disk_usage_none():
    assert format_disk_usage(None) == "-"


def test_format_disk_usage_bad_type():
    assert format_disk_usage("not-a-number") == "-"
    assert format_disk_usage(object()) == "-"


def test_format_disk_usage_kilobytes():
    assert format_disk_usage(512) == "512K"
    assert format_disk_usage(0) == "0K"


def test_format_disk_usage_megabytes():
    # 2048 KB → 2.0 MB
    assert format_disk_usage(2048) == "2.0M"


def test_format_disk_usage_gigabytes():
    # 1024*1024*2 KB → 2.00 G
    assert format_disk_usage(1024 * 1024 * 2) == "2.00G"


def test_format_disk_usage_boundary_1024_is_mb():
    # 1024 KB == 1.0 M (not K, since condition is kb_f < 1024)
    assert format_disk_usage(1024) == "1.0M"


# ─────────────────────────────────────────────────────────────────────────────
# 10. PoolSummary
# ─────────────────────────────────────────────────────────────────────────────


def test_pool_summary_defaults():
    p = PoolSummary()
    assert p.total_slots == 0
    assert p.idle_slots == 0
    assert p.claimed_slots == 0
    assert p.other_slots == 0
    assert p.total_cpus == 0
    assert p.load_avg == 0.0
    assert p.os_arch == ""
    assert p.slots == []


def test_pool_summary_to_dict_excludes_slots():
    p = PoolSummary(
        total_slots=3,
        idle_slots=1,
        os_arch="LINUX/X86_64",
        slots=[{"Name": "s1"}],
    )
    d = p.to_dict()
    assert "slots" not in d
    assert d["total_slots"] == 3
    assert d["idle_slots"] == 1
    assert d["os_arch"] == "LINUX/X86_64"


def test_pool_summary_from_dict_roundtrip():
    p = PoolSummary(
        total_slots=5,
        idle_slots=2,
        claimed_slots=2,
        other_slots=1,
        total_cpus=20,
        idle_cpus=8,
        total_memory_mb=40960,
        idle_memory_mb=16384,
        total_disk_kb=1234,
        total_gpus=4,
        idle_gpus=2,
        machines=3,
        load_avg=2.5,
        os_arch="LINUX/X86_64",
    )
    restored = PoolSummary.from_dict(p.to_dict())
    assert restored.to_dict() == p.to_dict()
    # raw slots are not part of the serialized round-trip
    assert restored.slots == []


def test_pool_summary_from_dict_tolerates_missing_keys():
    p = PoolSummary.from_dict({"total_slots": 7})
    assert p.total_slots == 7
    assert p.idle_slots == 0
    assert p.load_avg == 0.0
    assert p.os_arch == ""
    assert p.machines == 0


def test_pool_summary_from_dict_empty():
    p = PoolSummary.from_dict({})
    assert p == PoolSummary()


# ─────────────────────────────────────────────────────────────────────────────
# 11. _summarize_slots
# ─────────────────────────────────────────────────────────────────────────────


def test_summarize_static_slots_from_fixture(condor_slot_ads):
    s = hp._summarize_slots(condor_slot_ads)
    # 2 static slots: one Claimed/Busy, one Unclaimed/Idle
    assert s.total_slots == 2
    assert s.claimed_slots == 1
    assert s.idle_slots == 1
    assert s.other_slots == 0
    assert s.total_cpus == 8
    assert s.idle_cpus == 4  # only the idle slot
    assert s.total_memory_mb == 16384
    assert s.idle_memory_mb == 8192
    assert s.total_disk_kb == 2 * 1048576
    assert s.machines == 2
    assert s.load_avg == 0.0  # static slots do not add load (only partitionable)
    assert s.os_arch == "LINUX/X86_64"


def test_summarize_slots_keeps_raw_slots():
    slots = [{"SlotType": "Static", "State": "Claimed", "Cpus": 1}]
    s = hp._summarize_slots(slots)
    assert s.slots is slots


def test_summarize_slots_idle_via_activity():
    # State not Unclaimed but Activity Idle → counted as idle
    slots = [
        {
            "SlotType": "Static",
            "State": "Owner",
            "Activity": "Idle",
            "Cpus": 2,
            "Memory": 1000,
            "GPUs": 1,
        }
    ]
    s = hp._summarize_slots(slots)
    assert s.idle_slots == 1
    assert s.claimed_slots == 0
    assert s.idle_cpus == 2
    assert s.idle_memory_mb == 1000
    assert s.idle_gpus == 1


def test_summarize_slots_other_state():
    # Claimed handled separately; anything else (not idle/claimed) → other
    slots = [
        {
            "SlotType": "Static",
            "State": "Preempting",
            "Activity": "Vacating",
            "Cpus": 4,
        }
    ]
    s = hp._summarize_slots(slots)
    assert s.other_slots == 1
    assert s.idle_slots == 0
    assert s.claimed_slots == 0
    assert s.total_cpus == 4
    assert s.idle_cpus == 0


def test_summarize_slots_machine_dedup():
    slots = [
        {"SlotType": "Static", "State": "Claimed", "Machine": "m1", "Cpus": 1},
        {"SlotType": "Static", "State": "Claimed", "Machine": "m1", "Cpus": 1},
        {"SlotType": "Static", "State": "Claimed", "Machine": "m2", "Cpus": 1},
    ]
    s = hp._summarize_slots(slots)
    assert s.machines == 2
    assert s.total_slots == 3


def test_summarize_slots_os_arch_sorted_join():
    slots = [
        {
            "SlotType": "Static",
            "State": "Claimed",
            "OpSys": "WINDOWS",
            "Arch": "X86_64",
        },
        {"SlotType": "Static", "State": "Claimed", "OpSys": "LINUX", "Arch": "X86_64"},
        {"SlotType": "Static", "State": "Claimed", "OpSys": "LINUX", "Arch": "X86_64"},
    ]
    s = hp._summarize_slots(slots)
    # sorted unique, comma-space joined
    assert s.os_arch == "LINUX/X86_64, WINDOWS/X86_64"


def test_summarize_slots_partitionable_not_counted_but_contributes():
    slots = [
        {
            "SlotType": "Partitionable",
            "Machine": "pmachine.example.org",
            "TotalLoadAvg": 1.5,
            "OpSys": "LINUX",
            "Arch": "X86_64",
            # available (idle) resources
            "Cpus": 6,
            "Memory": 12000,
            "Disk": 500000,
            "GPUs": 2,
            # total capacity of the machine
            "TotalSlotCpus": 16,
            "TotalSlotMemory": 32000,
        }
    ]
    s = hp._summarize_slots(slots)
    # NOT counted as a slot
    assert s.total_slots == 0
    assert s.idle_slots == 0
    assert s.claimed_slots == 0
    # but contributes machine, load, totals, idle, disk, gpus
    assert s.machines == 1
    assert s.load_avg == pytest.approx(1.5)
    assert s.total_cpus == 16  # from TotalSlotCpus
    assert s.total_memory_mb == 32000  # from TotalSlotMemory
    assert s.idle_cpus == 6  # from Cpus
    assert s.idle_memory_mb == 12000  # from Memory
    assert s.total_disk_kb == 500000
    assert s.total_gpus == 2
    assert s.idle_gpus == 2
    assert s.os_arch == "LINUX/X86_64"


def test_summarize_slots_partitionable_falls_back_to_cpus_memory():
    # No TotalSlotCpus/TotalSlotMemory → falls back to Cpus/Memory
    slots = [
        {
            "SlotType": "Partitionable",
            "Machine": "p2",
            "Cpus": 8,
            "Memory": 4000,
        }
    ]
    s = hp._summarize_slots(slots)
    assert s.total_cpus == 8
    assert s.total_memory_mb == 4000
    assert s.idle_cpus == 8
    assert s.idle_memory_mb == 4000


def test_summarize_slots_mixed_partitionable_and_dynamic():
    slots = [
        # partitionable parent on host A
        {
            "SlotType": "Partitionable",
            "Machine": "hostA",
            "TotalLoadAvg": 2.0,
            "OpSys": "LINUX",
            "Arch": "X86_64",
            "Cpus": 2,  # leftover idle on parent
            "Memory": 2048,
            "Disk": 100000,
            "GPUs": 0,
            "TotalSlotCpus": 8,
            "TotalSlotMemory": 16384,
        },
        # dynamic child slot, claimed, on host A
        {
            "SlotType": "Dynamic",
            "Machine": "hostA",
            "State": "Claimed",
            "Activity": "Busy",
            "Cpus": 4,
            "Memory": 8192,
            "Disk": 50000,
            "GPUs": 0,
            "OpSys": "LINUX",
            "Arch": "X86_64",
        },
        # static idle slot on host B
        {
            "SlotType": "Static",
            "Machine": "hostB",
            "State": "Unclaimed",
            "Activity": "Idle",
            "Cpus": 4,
            "Memory": 4096,
            "Disk": 200000,
            "GPUs": 1,
            "OpSys": "LINUX",
            "Arch": "X86_64",
        },
    ]
    s = hp._summarize_slots(slots)
    # Only the dynamic + static count as slots (parent does not)
    assert s.total_slots == 2
    assert s.claimed_slots == 1
    assert s.idle_slots == 1
    assert s.other_slots == 0
    # machines: hostA (from parent+dynamic) + hostB
    assert s.machines == 2
    # load from partitionable only
    assert s.load_avg == pytest.approx(2.0)
    # total cpus: parent TotalSlotCpus 8 + dynamic 4 + static 4 = 16
    assert s.total_cpus == 16
    # idle cpus: parent Cpus 2 + static idle 4 = 6 (dynamic claimed not idle)
    assert s.idle_cpus == 6
    # total memory: parent 16384 + dynamic 8192 + static 4096 = 28672
    assert s.total_memory_mb == 28672
    # idle memory: parent 2048 + static 4096 = 6144
    assert s.idle_memory_mb == 6144
    # disk: 100000 + 50000 + 200000
    assert s.total_disk_kb == 350000
    # gpus: parent 0 + dynamic 0 + static 1; idle gpus parent 0 + static idle 1
    assert s.total_gpus == 1
    assert s.idle_gpus == 1
    assert s.os_arch == "LINUX/X86_64"


def test_summarize_slots_bad_numeric_values_ignored():
    # Non-numeric resource fields are swallowed, not crashed on.
    slots = [
        {
            "SlotType": "Static",
            "State": "Claimed",
            "Cpus": "bad",
            "Memory": "bad",
            "Disk": "bad",
            "GPUs": "bad",
        }
    ]
    s = hp._summarize_slots(slots)
    assert s.total_slots == 1
    assert s.total_cpus == 0
    assert s.total_memory_mb == 0
    assert s.total_disk_kb == 0
    assert s.total_gpus == 0


def test_summarize_slots_empty():
    s = hp._summarize_slots([])
    assert s.total_slots == 0
    assert s.machines == 0
    assert s.os_arch == ""


# ─────────────────────────────────────────────────────────────────────────────
# 12. Subprocess query functions — query_queue / _query_via_subprocess
# ─────────────────────────────────────────────────────────────────────────────


def test_query_queue_valid_json(no_bindings, monkeypatch, condor_queue_ads):
    monkeypatch.setattr(
        hp.subprocess,
        "run",
        lambda *a, **k: fake_proc(0, json.dumps(condor_queue_ads)),
    )
    result = query_queue()
    assert result == condor_queue_ads


def test_query_queue_empty_stdout_returns_empty(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, ""))
    assert query_queue() == []


def test_query_queue_empty_stdout_never_raises_even_with_flag(no_bindings, monkeypatch):
    # Contract: empty-but-valid result (exit 0, no stdout) returns [] and
    # NEVER raises, even with raise_on_error=True.
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, "   \n  "))
    assert query_queue(raise_on_error=True) == []


def test_query_queue_non_list_json_returns_empty(no_bindings, monkeypatch):
    monkeypatch.setattr(
        hp.subprocess, "run", lambda *a, **k: fake_proc(0, json.dumps({"k": "v"}))
    )
    assert query_queue() == []


def test_query_queue_nonzero_returncode_default_empty(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(1, "", "boom"))
    assert query_queue() == []


def test_query_queue_nonzero_returncode_raises_with_flag(no_bindings, monkeypatch):
    monkeypatch.setattr(
        hp.subprocess, "run", lambda *a, **k: fake_proc(1, "", "schedd down")
    )
    with pytest.raises(CondorQueryError):
        query_queue(raise_on_error=True)


def test_query_queue_filenotfound_default_empty(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("condor_q")

    monkeypatch.setattr(hp.subprocess, "run", boom)
    assert query_queue() == []


def test_query_queue_filenotfound_raises_with_flag(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("condor_q")

    monkeypatch.setattr(hp.subprocess, "run", boom)
    with pytest.raises(CondorQueryError):
        query_queue(raise_on_error=True)


def test_query_queue_timeout_default_empty(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="condor_q", timeout=10)

    monkeypatch.setattr(hp.subprocess, "run", boom)
    assert query_queue() == []


def test_query_queue_timeout_raises_with_flag(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="condor_q", timeout=10)

    monkeypatch.setattr(hp.subprocess, "run", boom)
    with pytest.raises(CondorQueryError):
        query_queue(raise_on_error=True)


def test_query_queue_unparseable_json_default_empty(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, "{not json"))
    assert query_queue() == []


def test_query_queue_unparseable_json_raises_with_flag(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, "{not json"))
    with pytest.raises(CondorQueryError):
        query_queue(raise_on_error=True)


def test_query_queue_uses_bindings_result_without_subprocess(monkeypatch):
    sample = [{"ClusterId": 1, "JobStatus": 2}]
    monkeypatch.setattr(hp, "_try_python_bindings", lambda *a, **k: sample)

    def fail(*a, **k):
        raise AssertionError("subprocess.run must not be called")

    monkeypatch.setattr(hp.subprocess, "run", fail)
    assert query_queue() == sample


def test_query_queue_bindings_empty_list_short_circuits(monkeypatch):
    # An empty list from the bindings is non-None → returned directly,
    # subprocess fallback is NOT used.
    monkeypatch.setattr(hp, "_try_python_bindings", lambda *a, **k: [])

    def fail(*a, **k):
        raise AssertionError("subprocess.run must not be called")

    monkeypatch.setattr(hp.subprocess, "run", fail)
    assert query_queue() == []


def test_query_queue_command_includes_constraint_and_name(no_bindings, monkeypatch):
    captured = {}

    def capture(cmd, *a, **k):
        captured["cmd"] = cmd
        return fake_proc(0, "[]")

    monkeypatch.setattr(hp.subprocess, "run", capture)
    query_queue(constraint='Owner=="x"', schedd_name="sched1")
    cmd = captured["cmd"]
    assert cmd[0] == "condor_q"
    assert "-json" in cmd
    assert "-name" in cmd and "sched1" in cmd
    assert "-constraint" in cmd and 'Owner=="x"' in cmd


# ─────────────────────────────────────────────────────────────────────────────
# query_history / _history_via_subprocess
# ─────────────────────────────────────────────────────────────────────────────


def test_query_history_valid_json(no_bindings, monkeypatch, condor_history_ads):
    monkeypatch.setattr(
        hp.subprocess,
        "run",
        lambda *a, **k: fake_proc(0, json.dumps(condor_history_ads)),
    )
    assert query_history() == condor_history_ads


def test_query_history_empty_stdout(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, ""))
    assert query_history() == []


def test_query_history_non_list_json(no_bindings, monkeypatch):
    monkeypatch.setattr(
        hp.subprocess, "run", lambda *a, **k: fake_proc(0, json.dumps({"a": 1}))
    )
    assert query_history() == []


def test_query_history_nonzero_returncode_swallowed(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(1, "", "err"))
    assert query_history() == []


def test_query_history_filenotfound_swallowed(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("condor_history")

    monkeypatch.setattr(hp.subprocess, "run", boom)
    assert query_history() == []


def test_query_history_timeout_swallowed(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="condor_history", timeout=15)

    monkeypatch.setattr(hp.subprocess, "run", boom)
    assert query_history() == []


def test_query_history_unparseable_json_swallowed(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, "not json"))
    assert query_history() == []


def test_query_history_uses_bindings_result(monkeypatch):
    sample = [{"ClusterId": 9, "ExitCode": 0}]
    monkeypatch.setattr(hp, "_try_history_bindings", lambda *a, **k: sample)

    def fail(*a, **k):
        raise AssertionError("subprocess.run must not be called")

    monkeypatch.setattr(hp.subprocess, "run", fail)
    assert query_history() == sample


def test_query_history_command_includes_match(no_bindings, monkeypatch):
    captured = {}

    def capture(cmd, *a, **k):
        captured["cmd"] = cmd
        return fake_proc(0, "[]")

    monkeypatch.setattr(hp.subprocess, "run", capture)
    query_history(match=50)
    cmd = captured["cmd"]
    assert cmd[0] == "condor_history"
    assert "-match" in cmd and "50" in cmd


# ─────────────────────────────────────────────────────────────────────────────
# query_slots / _slots_via_subprocess
# ─────────────────────────────────────────────────────────────────────────────


def test_query_slots_valid_returns_summary(no_bindings, monkeypatch, condor_slot_ads):
    monkeypatch.setattr(
        hp.subprocess,
        "run",
        lambda *a, **k: fake_proc(0, json.dumps(condor_slot_ads)),
    )
    summary = query_slots()
    assert isinstance(summary, PoolSummary)
    assert summary.total_slots == 2
    assert summary.idle_slots == 1
    assert summary.claimed_slots == 1


def test_query_slots_empty_returns_none(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, "[]"))
    assert query_slots() is None


def test_query_slots_empty_stdout_returns_none(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, ""))
    assert query_slots() is None


def test_query_slots_nonzero_returncode_returns_none(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(1, "", "err"))
    assert query_slots() is None


def test_query_slots_filenotfound_returns_none(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("condor_status")

    monkeypatch.setattr(hp.subprocess, "run", boom)
    assert query_slots() is None


def test_query_slots_timeout_returns_none(no_bindings, monkeypatch):
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="condor_status", timeout=15)

    monkeypatch.setattr(hp.subprocess, "run", boom)
    assert query_slots() is None


def test_query_slots_unparseable_json_returns_none(no_bindings, monkeypatch):
    monkeypatch.setattr(hp.subprocess, "run", lambda *a, **k: fake_proc(0, "{bad"))
    assert query_slots() is None


def test_query_slots_non_list_json_returns_none(no_bindings, monkeypatch):
    monkeypatch.setattr(
        hp.subprocess, "run", lambda *a, **k: fake_proc(0, json.dumps({"x": 1}))
    )
    assert query_slots() is None


def test_query_slots_uses_bindings_result(monkeypatch, condor_slot_ads):
    monkeypatch.setattr(hp, "_try_slots_bindings", lambda *a, **k: condor_slot_ads)

    def fail(*a, **k):
        raise AssertionError("subprocess.run must not be called")

    monkeypatch.setattr(hp.subprocess, "run", fail)
    summary = query_slots()
    assert isinstance(summary, PoolSummary)
    assert summary.total_slots == 2


def test_query_slots_command_includes_pool(no_bindings, monkeypatch):
    captured = {}

    def capture(cmd, *a, **k):
        captured["cmd"] = cmd
        return fake_proc(0, "[]")

    monkeypatch.setattr(hp.subprocess, "run", capture)
    query_slots(collector_host="collector.example.org:9618")
    cmd = captured["cmd"]
    assert cmd[0] == "condor_status"
    assert "-pool" in cmd and "collector.example.org:9618" in cmd
