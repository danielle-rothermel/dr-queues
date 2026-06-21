import typer

from dr_queues import hello

app = typer.Typer(add_completion=False)


@app.command()
def main() -> None:
    typer.echo(hello())


if __name__ == "__main__":
    app()
