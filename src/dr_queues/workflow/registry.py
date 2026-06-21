from __future__ import annotations

from collections.abc import Callable

from dr_queues.pipeline.job import JobEnvelope

StepHandler = Callable[[JobEnvelope], JobEnvelope]


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, StepHandler] = {}

    def register(self, name: str) -> Callable[[StepHandler], StepHandler]:
        def decorator(handler: StepHandler) -> StepHandler:
            self._handlers[name] = handler
            return handler

        return decorator

    def get(self, name: str) -> StepHandler:
        if name not in self._handlers:
            msg = f"Unknown handler: {name}"
            raise ValueError(msg)
        return self._handlers[name]

    def has(self, name: str) -> bool:
        return name in self._handlers
