from __future__ import annotations

import random
import time

from dr_queues.pipeline.job import JobEnvelope
from dr_queues.workflow.registry import HandlerRegistry

registry = HandlerRegistry()


@registry.register("sleep_ms")
def sleep_ms(job: JobEnvelope) -> JobEnvelope:
    delay_ms = job.payload.get("sleep_ms", random.randint(50, 200))
    time.sleep(delay_ms / 1000.0)
    job.step_outputs["slow"] = f"slept_{delay_ms}ms"
    return job


@registry.register("add_prefix")
def add_prefix(job: JobEnvelope) -> JobEnvelope:
    prior = job.step_outputs.get("slow", "")
    prefix = job.payload.get("prefix", "transformed:")
    job.step_outputs["transform"] = f"{prefix}{prior}"
    return job


@registry.register("record_artifact")
def record_artifact(job: JobEnvelope) -> JobEnvelope:
    counter = int(job.payload.get("counter", 0)) + 1
    job.payload["counter"] = counter
    job.step_records["finalize"] = {
        "counter": counter,
        "lane": job.lane,
        "repeat": job.repeat,
    }
    job.step_outputs["finalize"] = "done"
    return job
