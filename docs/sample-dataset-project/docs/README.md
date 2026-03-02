# Sample Dataset — sales_orders

This is a **realistic sample dataset** to demonstrate the Governed Ingestion Platform.

## Business context
Represents customer sales orders with monetary values and update timestamps.

## Key characteristics
- Incremental load by `updated_at`
- Deterministic merge using Last Write Wins
- Decimal handling with precision/scale
- Fully governed Silver contract

## Lifecycle
- Created as DRAFT
- Published → ACTIVE
- Governed by ExpectSchemaJSON v1

## Generated at
2025-12-12T21:17:39.482235Z
