# Data quality expectations (duplicate checks)

Some tables look “duplicated” when you check a **single column**, but that is **expected** given the grain of the data.

## `order_items`

- **`order_id` repeats** across rows: normal. One order has many line items (many rows share the same `order_id`).
- Do **not** treat repeated `order_id` as bad full-row duplication without checking the full primary key (e.g. `order_id` + `item_id`).

## `stocks`

- **Repeating `product_id`** across rows: normal (same product stocked in **different stores**).
- **Repeating `store_id`** across rows: normal (each store stocks **many products**).
- Grain is typically **store × product** (one row per store–product with quantity). Checking only `product_id` or only `store_id` for duplicates will flag many rows; that does **not** mean the table is wrongly duplicated.

When validating `stocks`, assert uniqueness on **`(store_id, product_id)`** (or whatever your business key is), not on `product_id` or `store_id` alone.
