from __future__ import annotations

import json
from typing import Any


def load_json_body(body: bytes | None) -> dict[str, Any]:
    if body is None:
        return {}
    return json.loads(body.decode("utf-8"))
