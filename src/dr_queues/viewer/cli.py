from __future__ import annotations

import os

import typer

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
VIEWER_RUN_ID_ENV = "DR_QUEUES_VIEWER_RUN_ID"
VIEWER_EXTRA_HINT = (
    "The viewer requires optional dependencies. Install with "
    '`uv add "dr-queues[viewer]"` or `uv sync --extra viewer`.'
)

app = typer.Typer(add_completion=False)


@app.command()
def main(
    run_id: str | None = typer.Option(None, "--run-id"),
    host: str = typer.Option(DEFAULT_HOST, "--host"),
    port: int = typer.Option(DEFAULT_PORT, "--port"),
    reload: bool = typer.Option(False, "--reload"),
) -> None:
    try:
        import uvicorn

        __import__("dr_queues.viewer.app")
    except ImportError as error:
        typer.echo(VIEWER_EXTRA_HINT, err=True)
        raise typer.Exit(code=1) from error

    if run_id is not None:
        os.environ[VIEWER_RUN_ID_ENV] = run_id

    typer.echo(f"dr-queues viewer: http://{host}:{port}")
    uvicorn.run(
        "dr_queues.viewer.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
    )


def run() -> None:
    app()


if __name__ == "__main__":
    run()
