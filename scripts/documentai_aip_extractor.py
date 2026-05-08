from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.cloud import documentai_v1 as documentai
from google.protobuf.json_format import MessageToDict

from app.services.documentai.aip_documentai import (
    DocumentAiConfig,
    _process_pdf,
    collapse_schema_entities,
    entity_types_histogram,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract AIP entities with Document AI.")
    parser.add_argument("file_path", help="Local PDF path")
    parser.add_argument("--project-id", default="655673003934")
    parser.add_argument("--location", default="us")
    parser.add_argument("--processor-id", default="c57cf2f52aedbbe3")
    parser.add_argument("--processor-version-id", default=None)
    parser.add_argument("--imageless-mode", action="store_true")
    parser.add_argument("--dump", choices=("nested", "entities", "full"), default="nested")
    parser.add_argument("--include-text", action="store_true")
    parser.add_argument("--print-json", action="store_true")
    parser.add_argument("--debug-summary", action="store_true")
    return parser.parse_args()


def _export(document: documentai.Document, dump: str, include_text: bool) -> dict:
    if dump == "full":
        return MessageToDict(document._pb, preserving_proto_field_name=True)
    if dump == "entities":
        output = {
            "entities": [
                MessageToDict(entity._pb, preserving_proto_field_name=True)
                for entity in document.entities
            ]
        }
        if include_text:
            output["text"] = document.text
        return output
    output = {
        "schema_tree": collapse_schema_entities(document.entities),
        "entity_root_count": len(document.entities),
    }
    if include_text:
        output["text"] = document.text
    return output


def main() -> None:
    args = _parse_args()
    config = DocumentAiConfig(
        project_id=args.project_id,
        location=args.location,
        processor_id=args.processor_id,
        processor_version_id=args.processor_version_id,
        imageless_mode=args.imageless_mode,
    )
    document = _process_pdf(Path(args.file_path), config)
    if args.debug_summary:
        print(json.dumps(entity_types_histogram(document), ensure_ascii=False, indent=2))
    result = _export(document, args.dump, args.include_text)
    output_path = Path(args.file_path).stem + "_extracted.json"
    Path(output_path).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Resultado guardado en: {output_path}")
    if args.print_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
