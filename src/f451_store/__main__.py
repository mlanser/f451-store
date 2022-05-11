"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """f451 Datastore."""


if __name__ == "__main__":
    main(prog_name="f451-store")  # pragma: no cover
