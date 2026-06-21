from __future__ import annotations

import shutil
import sys

STAGE_WORKER_ENTRYPOINT = "dr-queues-stage-worker"


def stage_worker_command_prefix() -> list[str]:
    """Resolve the stage-worker CLI for installed or editable runs."""
    executable = shutil.which(STAGE_WORKER_ENTRYPOINT)
    if executable is not None:
        return [executable]
    return [sys.executable, "-m", "dr_queues.cli.stage_worker"]
