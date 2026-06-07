"""CLI entry point for ad-extractor."""

import json
import sys
from pathlib import Path

import typer
from dotenv import load_dotenv

from .pipeline import run
from .schemas import SUPPORTED_SECTIONS

load_dotenv()

app = typer.Typer(
    name="ad-extractor",
    help="Extract structured AIP aerodrome data from PDF using Vertex AI Gemini.",
    no_args_is_help=True,
)


def _parse_sections(sections: str | None) -> list[str] | None:
    if not sections:
        return None
    return [s.strip() for s in sections.split(",") if s.strip()]


@app.command()
def main(
    pdf: Path = typer.Argument(..., help="Path to the AIP PDF file"),
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Write JSON to this file (default: stdout)",
    ),
    sections: str | None = typer.Option(
        None,
        "--sections",
        "-s",
        help=f"Comma-separated sections to extract (default: all). "
        f"Supported: {', '.join(SUPPORTED_SECTIONS)}",
    ),
) -> None:
    """Extract AD 2.X sections from an AIP PDF."""
    if not pdf.exists():
        typer.echo(f"Error: PDF not found: {pdf}", err=True)
        raise typer.Exit(code=1)

    section_list = _parse_sections(sections)

    typer.echo(f"Processing {pdf}...", err=True)
    if section_list:
        typer.echo(f"Sections: {', '.join(section_list)}", err=True)
    else:
        typer.echo(f"Sections: all ({len(SUPPORTED_SECTIONS)})", err=True)

    try:
        result = run(pdf, sections=section_list)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    json_str = json.dumps(result, ensure_ascii=False, indent=2)

    if output:
        output.write_text(json_str, encoding="utf-8")
        typer.echo(f"Written to {output}", err=True)
    else:
        # UTF-8 stdout on Windows may need reconfigure
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        typer.echo(json_str)

    if result.get("errors"):
        typer.echo(
            f"Warning: {len(result['errors'])} section(s) failed — see 'errors' in output",
            err=True,
        )
        raise typer.Exit(code=2)

    if result.get("field_errors"):
        typer.echo(
            f"Warning: {len(result['field_errors'])} field(s) with ERROR_EXTRACCION "
            "— see 'field_errors' in output",
            err=True,
        )
        raise typer.Exit(code=2)

    if result.get("sections_skipped"):
        typer.echo(
            f"Note: skipped missing sections: {', '.join(result['sections_skipped'])}",
            err=True,
        )


if __name__ == "__main__":
    app()
