"""Pipeline orchestration for deterministic AD 2.0 parsing stages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class ExtractionTelemetry:
    extraction_seconds: float
    ocr_seconds: float
    ocr_triggered: bool


@dataclass(frozen=True)
class PipelineSectionsResult:
    text: str
    sections: list[Any]


def extract_and_segment_sections(
    *,
    pdf_paths: list[Path],
    icao: str,
    config: Any,
    parser_factory: Callable[[Any], Any],
    extract_documents_text: Callable[[Any, list[Path]], tuple[str, float, float, bool]],
    segment_sections: Callable[[str, Path, str], list[Any]],
    forced_config_builder: Callable[[Any], Any],
    format_error: type[Exception],
    logger: Any,
) -> PipelineSectionsResult:
    parser = parser_factory(config)
    text, total_extraction_seconds, total_ocr_seconds, any_ocr_triggered = extract_documents_text(
        parser, pdf_paths
    )
    _log_extraction(
        logger,
        event="aip.parser.extraction",
        strategy="docling_ocr",
        telemetry=ExtractionTelemetry(
            extraction_seconds=total_extraction_seconds,
            ocr_seconds=total_ocr_seconds,
            ocr_triggered=any_ocr_triggered,
        ),
        documents_count=len(pdf_paths),
    )

    try:
        sections = segment_sections(text, pdf_paths[0], icao)
    except Exception as first_exc:
        if not isinstance(first_exc, format_error):
            raise format_error(
                f"Deterministic AD 2.0 segmentation failed for '{pdf_paths[0]}': {first_exc}"
            ) from first_exc

        if not config.ocr_enabled:
            raise format_error(
                f"Deterministic AD 2.0 segmentation failed for '{pdf_paths[0]}': {first_exc}"
            ) from first_exc

        forced_config = forced_config_builder(config)
        forced_parser = parser_factory(forced_config)
        try:
            retry_text, retry_extraction_seconds, retry_ocr_seconds, retry_ocr_triggered = extract_documents_text(
                forced_parser, pdf_paths
            )
            _log_extraction(
                logger,
                event="aip.parser.extraction.retry",
                strategy="docling_ocr_document_forced",
                telemetry=ExtractionTelemetry(
                    extraction_seconds=retry_extraction_seconds,
                    ocr_seconds=retry_ocr_seconds,
                    ocr_triggered=retry_ocr_triggered,
                ),
                documents_count=len(pdf_paths),
            )
            sections = segment_sections(retry_text, pdf_paths[0], icao)
            text = retry_text
        except Exception as retry_exc:
            raise format_error(
                f"Deterministic AD 2.0 segmentation failed for '{pdf_paths[0]}': {first_exc} "
                f"| retry_with_forced_document_ocr_failed: {retry_exc}"
            ) from retry_exc

    return PipelineSectionsResult(text=text, sections=sections)


def _log_extraction(
    logger: Any,
    *,
    event: str,
    strategy: str,
    telemetry: ExtractionTelemetry,
    documents_count: int,
) -> None:
    logger.info(
        event,
        extra={
            "strategy": strategy,
            "ocr_triggered": telemetry.ocr_triggered,
            "documents_count": documents_count,
            "extraction_seconds": round(telemetry.extraction_seconds, 3),
            "ocr_seconds": round(telemetry.ocr_seconds, 3),
        },
    )
