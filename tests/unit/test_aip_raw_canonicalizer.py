from app.services.scraper.aip_raw_canonicalizer import canonicalize_section_raw_text


def test_canonicalize_numbered_section_to_item_value_blocks() -> None:
    raw = """AD 2.3 HORAS DE FUNCIONAMIENTO / OPERATIONAL HOURS
1 Explotador del AD / AD operator 11:00-19:00 UTC
2 Aduanas / Customs No
12 Observaciones / Remarks (*) COOR (+54 260) 4824047
"""

    output = canonicalize_section_raw_text("AD 2.3", raw)

    assert output.startswith("SECTION: AD 2.3 | HORAS DE FUNCIONAMIENTO / OPERATIONAL HOURS")
    assert "ITEM: 1 | Explotador del AD / AD operator" in output
    assert "VALUE: 11:00-19:00 UTC" in output
    assert "ITEM: 12 | Observaciones / Remarks" in output
    assert "VALUE: (*) COOR (+54 260) 4824047" in output


def test_canonicalize_ad220_keeps_bullets_as_notes() -> None:
    raw = """AD 2.20 REGLAMENTO LOCAL DEL AERODROMO / LOCAL AERODROME REGULATIONS
- Las OPS VFR deberan ajustarse a lo establecido en ENR 1.4
- VFR OPS shall conform to BRAVO ANNEX
"""

    output = canonicalize_section_raw_text("AD 2.20", raw)

    assert output.startswith("SECTION: AD 2.20 | REGLAMENTO LOCAL DEL AERODROMO / LOCAL AERODROME REGULATIONS")
    assert "NOTE: - Las OPS VFR deberan ajustarse a lo establecido en ENR 1.4" in output
    assert "NOTE: - VFR OPS shall conform to BRAVO ANNEX" in output


def test_canonicalize_ad225_sets_no_as_value() -> None:
    raw = """AD 2.25 PENETRACION DE LA SUPERFICIE DEL TRAMO VISUAL (VSS) / VISUAL SEGMENT SURFACE (VSS) PENETRATION
No
"""

    output = canonicalize_section_raw_text("AD 2.25", raw)

    assert output.startswith(
        "SECTION: AD 2.25 | PENETRACION DE LA SUPERFICIE DEL TRAMO VISUAL (VSS) / VISUAL SEGMENT SURFACE (VSS) PENETRATION"
    )
    assert "VALUE: No" in output


def test_canonicalize_pipe_table_row_as_item_value() -> None:
    raw = """AD 2.19 RADIOAYUDAS PARA LA NAVEGACION Y EL ATERRIZAJE / NAVIGATIONAL AND LANDING AIDS
| 1 | Tipo de ayuda / Type of aid | VOR/DME |
| 2 | ID | SRA |
| 3 | Frecuencia y Canal / Frequency and channel | 116.9 MHz |
"""

    output = canonicalize_section_raw_text("AD 2.19", raw)

    assert "ITEM: 1 | Tipo de ayuda / Type of aid" in output
    assert "VALUE: VOR/DME" in output
    assert "ITEM: 3 | Frecuencia y Canal / Frequency and channel" in output
    assert "VALUE: 116.9 MHz" in output


def test_canonicalize_ad224_hierarchical_rows_keep_full_information() -> None:
    raw = """AD 2.24 CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME
Plano de aerodromo - OACI / Aerodrome Chart - ICAO SAMR AD 2.A1
Plano de estacionamiento y atraque de aeronaves - OACI / Aircraft Parking/Docking Chart - ICAO SAMR AD 2.B1-B2-B3-B4
Cartas de aproximacion por instrumentos - OACI / Instrument Approach Chart - ICAO
VOR Z PISTA/RWY 29 SAMR AD 2.M1
VOR PISTA/RWY 11 SAMR AD 2.M2
"""

    output = canonicalize_section_raw_text("AD 2.24", raw)

    assert "SECTION: AD 2.24 | CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME" in output
    assert "SAMR AD 2.A1" in output
    assert "SAMR AD 2.B1-B2-B3-B4" in output
    assert "SAMR AD 2.M1" in output
    assert "SAMR AD 2.M2" in output
    assert "ITEM: CHART | Plano de aerodromo - OACI / Aerodrome Chart - ICAO" in output
    assert "VALUE: SAMR AD 2.A1" in output


def test_canonicalize_non_numbered_pipe_rows_to_chart_items() -> None:
    raw = """AD 2.24 CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME
| Plano de aerodromo - OACI / Aerodrome Chart - ICAO | Plano de aerodromo - OACI / Aerodrome Chart - ICAO | SAMR AD 2.A1 |
| VOR Z | PISTA/ RWY 29 | SAMR AD 2.M1 |
"""

    output = canonicalize_section_raw_text("AD 2.24", raw)

    assert "ITEM: CHART | Plano de aerodromo - OACI / Aerodrome Chart - ICAO" in output
    assert "VALUE: SAMR AD 2.A1" in output
    assert "ITEM: CHART | VOR Z | PISTA/ RWY 29" in output
    assert "VALUE: SAMR AD 2.M1" in output


def test_canonicalize_drops_rule_like_pipe_separator_rows() -> None:
    raw = """AD 2.24 CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME
|------------------|------------------|------------------|
| VOR Z | PISTA/ RWY 29 | SAMR AD 2.M1 |
"""

    output = canonicalize_section_raw_text("AD 2.24", raw)

    assert "ITEM: CHART | VOR Z | PISTA/ RWY 29" in output
    assert "VALUE: SAMR AD 2.M1" in output
    assert "------------------" not in output


def test_canonicalize_four_column_pipe_rows_to_row_items() -> None:
    raw = """AD 2.10 OBSTACULOS DEL AERODROMO / AERODROME OBSTACLES
| RWY 29 - Ascenso en el despegue / takeoff climb | Arboleda/Grove, 800.0 m (2.625 ft) | 343445.2S 0682551.1W | NIL |
"""

    output = canonicalize_section_raw_text("AD 2.10", raw)

    assert "ITEM: ROW | RWY 29 - Ascenso en el despegue / takeoff climb" in output
    assert "VALUE: Arboleda/Grove, 800.0 m (2.625 ft) | 343445.2S 0682551.1W | NIL" in output


def test_canonicalize_skips_numeric_column_index_pipe_rows() -> None:
    raw = """AD 2.18 INSTALACIONES DE COMUNICACIONES DE LOS ATS / ATS COMMUNICATION FACILITIES
| 1 | 2 | 3 | 4 | 5 | 6 |
| TMA/APP/TWR | San Rafael Torre | CPPL | 118.10 MHz | H24 | NIL |
"""

    output = canonicalize_section_raw_text("AD 2.18", raw)

    assert "ITEM: 1 | 2" not in output
    assert "NOTE: | 1 | 2 | 3 | 4 | 5 | 6 |" not in output
    assert "ITEM: ROW | TMA/APP/TWR" in output
    assert "VALUE: San Rafael Torre | CPPL | 118.10 MHz | H24 | NIL" in output


def test_canonicalize_row_deduplicates_first_repeated_column() -> None:
    raw = """AD 2.19 RADIOAYUDAS / NAVIGATIONAL AIDS
| VOR/DME | VOR/DME | SRA | 116.9 MHz | H24 | 343522.00S 0682341.00W |
"""

    output = canonicalize_section_raw_text("AD 2.19", raw)

    assert "ITEM: ROW | VOR/DME" in output
    assert "VALUE: SRA | 116.9 MHz | H24 | 343522.00S 0682341.00W" in output
    assert "VALUE: VOR/DME | SRA" not in output


def test_canonicalize_does_not_merge_valid_spanish_words() -> None:
    raw = """AD 2.11 INFORMACION METEOROLOGICA PROPORCIONADA / METEOROLOGICAL INFORMATION PROVIDED
9 Dependencias ATS a las cuales se suministra informacion meteorologica / The ATS units provided with meteorological information TWR
"""

    output = canonicalize_section_raw_text("AD 2.11", raw)

    assert "Dependencias ATS a las cuales" in output
    assert "Dependencias ATS alas cuales" not in output


def test_canonicalize_extracts_title_from_markdown_header() -> None:
    raw = """## AD 2.12 CARACTERISTICAS FISICAS DE LAS PISTAS / RUNWAY PHYSICAL CHARACTERISTICS
Observaciones / Remarks: test
| 11 | 108.46°, 109° | 2.102x30 | 430/F/B/X/T ASPH |
"""

    output = canonicalize_section_raw_text("AD 2.12", raw)

    assert output.startswith(
        "SECTION: AD 2.12 | CARACTERISTICAS FISICAS DE LAS PISTAS / RUNWAY PHYSICAL CHARACTERISTICS"
    )


def test_canonicalize_deduplicates_value_when_equal_to_item_label() -> None:
    raw = """AD 2.4 SERVICIOS E INSTALACIONES DE ESCALA / HANDLING SERVICES AND FACILITIES
| 1 | Elementos disponibles para el manejo de carga Cargo-handling facilities | Elementos disponibles para el manejo de carga Cargo-handling facilities | No |
"""

    output = canonicalize_section_raw_text("AD 2.4", raw)

    assert "ITEM: 1 | Elementos disponibles para el manejo de carga Cargo-handling facilities" in output
    assert (
        "VALUE: Elementos disponibles para el manejo de carga Cargo-handling facilities | No" not in output
    )
    assert "VALUE: No" in output


def test_canonicalize_ad210_subheader_rows_are_notes_not_data_rows() -> None:
    raw = """AD 2.10 OBSTACULOS DEL AERODROMO / AERODROME OBSTACLES
| En las areas de aproximacion y despegue / In approach/TKOF areas | En las areas de aproximacion y despegue / In approach/TKOF areas | En las areas de aproximacion y despegue / In approach/TKOF areas |
| RWY 29 - Ascenso en el despegue / takeoff climb | Arboleda/Grove, 800.0 m (2.625 ft) | 343445.2S 0682551.1W |
"""

    output = canonicalize_section_raw_text("AD 2.10", raw)

    assert "ITEM: ROW | En las areas de aproximacion y despegue / In approach/TKOF areas" not in output
    assert "NOTE: En las areas de aproximacion y despegue / In approach/TKOF areas" in output
    assert "ITEM: ROW | RWY 29 - Ascenso en el despegue / takeoff climb" in output


def test_canonicalize_ad212_column_headers_are_notes() -> None:
    raw = """AD 2.12 CARACTERISTICAS FISICAS DE LAS PISTAS / RUNWAY PHYSICAL CHARACTERISTICS
| Designador RWY / RWY designation | BRG GEO, BRG MAG | Dimensiones de RWY (m) |
| 11 | 108.46°, 109° | 2.102x30 |
"""

    output = canonicalize_section_raw_text("AD 2.12", raw)

    assert "ITEM: CHART | Designador RWY / RWY designation | BRG GEO, BRG MAG" not in output
    assert "NOTE: Designador RWY / RWY designation | BRG GEO, BRG MAG | Dimensiones de RWY (m)" in output
    assert "ITEM: 11 | 108.46°, 109°" in output
    assert "VALUE: 2.102x30" in output


def test_canonicalize_ad213_rows_keep_designator_in_item_row() -> None:
    raw = """AD 2.13 DISTANCIAS DECLARADAS / DECLARED DISTANCES
| 11 | 2.102 | 2.102 | 2.102 | 2.102 | NIL |
| 29 | 2.102 | 2.102 | 2.102 | 2.102 | NIL |
"""

    output = canonicalize_section_raw_text("AD 2.13", raw)

    assert "ITEM: ROW | 11" in output
    assert "VALUE: 2.102 | 2.102 | 2.102 | 2.102 | NIL" in output
    assert "ITEM: ROW | 29" in output


def test_canonicalize_extracts_split_title_from_pipe_header_row() -> None:
    raw = """| AD 2.5 INSTALACIONES Y SERVICIOS PARA | AD 2.5 INSTALACIONES Y SERVICIOS PARA | LOS PASAJEROS / PASSENGER FACILITIES | LOS PASAJEROS / PASSENGER FACILITIES |
| 1 | Hoteles / Hotels | Hoteles / Hotels | Si / Yes |
"""

    output = canonicalize_section_raw_text("AD 2.5", raw)

    assert output.startswith(
        "SECTION: AD 2.5 | INSTALACIONES Y SERVICIOS PARA LOS PASAJEROS / PASSENGER FACILITIES"
    )


def test_canonicalize_ad225_pipe_scalar_no_as_value() -> None:
    raw = """AD 2.25 PENETRACION DE LA SUPERFICIE DEL TRAMO VISUAL (VSS) / VISUAL SEGMENT SURFACE (VSS) PENETRATION
| No |
"""

    output = canonicalize_section_raw_text("AD 2.25", raw)

    assert "VALUE: No" in output
    assert "NOTE: No" not in output


def test_canonicalize_ad211_drops_coordinate_noise_notes() -> None:
    raw = """1 Oficina MET asociada / Associated MET office EMA SAN RAFAEL
Coordenadas
/ Coordinates
343510.2S 0682717.9W
"""

    output = canonicalize_section_raw_text("AD 2.11", raw)

    assert "ITEM: 1 | Oficina MET asociada / Associated MET office EMA SAN RAFAEL" in output
    assert "VALUE: Coordenadas / Coordinates 343510.2S 0682717.9W" in output


def test_canonicalize_ad214_second_block_reuses_runway_designators() -> None:
    raw = """AD 2.14 LUCES DE APROXIMACION Y DE PISTA / APPROACH AND RUNWAY LIGHTING
| 11 | No | Verde, No | Si 3° | No |
| 29 | No | Verde, No | Si, 3° | No |
| LEN, Separacion, Color, INTST RCLL | LEN, Separacion, Color, INTST REDL | Color RENL y WBAR | LEN y Color STWL | Observaciones |
| No | 2.102 m, 60 m, blanco, LIH | Rojo, No | No | NIL |
| No | 2.102 m, 60 m, blanco, LIH | Rojo, No | No | NIL |
"""

    output = canonicalize_section_raw_text("AD 2.14", raw)

    assert "NOTE: LEN, Separacion, Color, INTST RCLL" in output
    assert "ITEM: ROW | 11" in output
    assert "ITEM: ROW | 29" in output
    assert "ITEM: ROW | No" not in output
    assert "VALUE: No | 2.102 m, 60 m, blanco, LIH | Rojo, No | No | NIL" in output


def test_canonicalize_normalizes_known_ocr_joined_tokens() -> None:
    raw = """AD 2.2 DATOS GEOGRAFICOS
6 Prestador ... EANASA
AD 2.11 INFORMACION METEOROLOGICA PROPORCIONADA
3 Oficina responsable ... OMAMENDOZA y OMACÓRDOBA
"""

    output = canonicalize_section_raw_text("AD 2.2", raw)

    assert "EANA SA" in output
    assert "OMAMENDOZA" not in output


def test_canonicalize_ad218_removes_trailing_slash_in_callsign_cell() -> None:
    raw = """AD 2.18 INSTALACIONES DE COMUNICACIONES DE LOS ATS / ATS COMMUNICATION FACILITIES
| TMA/APP/TWR | San Rafael Torre / | CPPL | 118.10 MHz | H24 |
"""

    output = canonicalize_section_raw_text("AD 2.18", raw)

    assert "VALUE: San Rafael Torre | CPPL | 118.10 MHz | H24" in output
    assert "San Rafael Torre / |" not in output
