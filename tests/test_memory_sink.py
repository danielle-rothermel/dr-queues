from threading import Thread
from uuid import uuid4

from dr_queues.events.memory import MemoryEventSink
from dr_queues.events.schema import EventKind, PipelineEvent


def test_append_and_read_by_run_id() -> None:
    sink = MemoryEventSink()
    event = PipelineEvent(
        run_id="run-1",
        job_id="job-1",
        lane="lane-a",
        stage="slow",
        event=EventKind.STAGE_STARTED,
    )
    sink.append(event)
    results = sink.read_by_run_id("run-1")
    assert len(results) == 1
    assert results[0].event_id == event.event_id


def test_thread_safe_append() -> None:
    sink = MemoryEventSink()
    run_id = f"run-{uuid4().hex[:8]}"

    def worker(index: int) -> None:
        sink.append(
            PipelineEvent(
                run_id=run_id,
                job_id=f"job-{index}",
                lane="lane-a",
                stage="slow",
                event=EventKind.STAGE_STARTED,
            ),
        )

    threads = [Thread(target=worker, args=(index,)) for index in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(sink.read_by_run_id(run_id)) == 20
