# Golden tests genericos para AIP AD 2

Fuente de verdad: `AIP-SAMR.pdf`.

Contrato `generic-field-value-v1`:
- Respetar `campo -> valor` tal como aparece en cada seccion AD 2.x del AIP.
- No atomizar un campo documental en subcampos semanticos.
- Usar `tables[]` solo cuando el documento presenta filas/columnas.
- Las columnas de tablas deben ser encabezados documentales, no nombres internos del dominio.
- `raw.txt` queda como insumo auxiliar; puede mejorarse sin cambiar este contrato.
