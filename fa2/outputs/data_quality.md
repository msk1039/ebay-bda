# Data quality report (Olist, Phase 1)

Source: Olist Brazilian E-Commerce Public Dataset (Kaggle). All numbers are computed by `analysis/p1_prepare.py` and stored in `outputs/metrics.json` (section `data_quality`).

## Summary

| Dataset | Raw rows | Parquet rows written |
|---|---:|---:|
| orders | 99,441 | 99,441 |
| order_items | 112,650 | 112,650 |
| customers | 99,441 | 99,441 |
| sellers | 3,095 | 3,095 |
| products | 32,951 | 32,951 |
| payments | 103,886 | 103,886 |
| reviews | 99,224 | 98,673 |
| geolocation | 1,000,163 | 19,010 |
| category_translation | 71 | (not written; used for join) |
| order_lines | (derived) | 112,650 |

## orders

- Rows (raw): **99,441**
- Key `order_id`: 99,441 distinct, 0 duplicated key values
- Nulls per column: `order_approved_at`=160, `order_delivered_carrier_date`=1,783, `order_delivered_customer_date`=2,965
- status distribution: delivered=96,478, shipped=1,107, canceled=625, unavailable=609, invoiced=314, processing=301, created=5, approved=2
- Purchase date range: 2016-09-05 05:45:19 to 2018-10-18 02:00:18
- Late deliveries: 6,535 of 96,476 orders with a delivery date (6.77%)
- Months with fewer than 100 orders (partial coverage): 2016-09=4, 2016-12=1, 2018-09=16, 2018-10=4

**Issues found**

- 8 orders with status 'delivered' have no delivery date
- 6 orders have a delivery date but a status other than 'delivered'
- 0 orders have delivery date earlier than purchase date
- 625 canceled and 609 unavailable orders
- 775 orders have no order_items row

**Decision:** Kept all rows (canceled/unavailable orders are real behaviour for demand analysis; analyses filter is_delivered when they need fulfilment). Delivery fields stay null when missing instead of being guessed.

## order_items

- Rows (raw): **112,650**
- Key `order_id,order_item_id`: 112,650 distinct, 0 duplicated key values
- Nulls per column: none

**Issues found**

- 0 items whose order_id is not in orders
- 0 items with price <= 0
- 0 items with negative freight

**Decision:** Removed duplicate (order_id, order_item_id) pairs and orphans if any; kept all prices (none <= 0).

## customers

- Rows (raw): **99,441**
- Key `customer_id`: 99,441 distinct, 0 duplicated key values
- Nulls per column: none

**Issues found**

- customer_id is per-order; 96,096 real people (customer_unique_id) behind 99,441 customer_ids

**Decision:** Trimmed/lower-cased city, upper-cased state; zip kept as string; no rows dropped.

## sellers

- Rows (raw): **3,095**
- Key `seller_id`: 3,095 distinct, 0 duplicated key values
- Nulls per column: none

**Issues found**

- no issues found: keys unique, no nulls

**Decision:** Same text normalisation as customers; no rows dropped.

## products

- Rows (raw): **32,951**
- Key `product_id`: 32,951 distinct, 0 duplicated key values
- Nulls per column: `product_category_name`=610, `product_name_lenght`=610, `product_description_lenght`=610, `product_photos_qty`=610, `product_weight_g`=2, `product_length_cm`=2, `product_height_cm`=2, `product_width_cm`=2

**Issues found**

- 610 products have no category name
- categories missing from the translation table: ['pc_gamer', 'portateis_cozinha_e_preparadores_de_alimentos']
- source columns product_name_lenght / product_description_lenght are misspelled (renamed to *_length)

**Decision:** Left-joined translation (no product lost). Missing category -> 'unknown'; the untranslated Portuguese names keep their original name rather than being hidden in 'unknown'. Typos in column names fixed.

## payments

- Rows (raw): **103,886**
- Key `order_id,payment_sequential`: 103,886 distinct, 0 duplicated key values
- Nulls per column: none
- payment types: credit_card=76,795, boleto=19,784, voucher=5,775, debit_card=1,529, not_defined=3

**Issues found**

- 3 rows with payment_type 'not_defined'
- 9 rows with payment_value = 0

**Decision:** Kept all rows incl. not_defined / zero-value (small, and they belong to real orders); an order may have several payment rows, so sum per order.

## reviews

- Rows (raw): **99,224**
- Key `review_id`: 98,410 distinct, 789 duplicated key values
- Nulls per column: `review_comment_title`=87,656, `review_comment_message`=58,247
- score distribution: 1=11,424, 2=3,151, 3=8,179, 4=19,142, 5=57,328

**Issues found**

- 789 duplicate key values (814 surplus rows) on ['review_id']
- 0 rows with a missing/out-of-range score (0 means the multiline parse is sane)
- 547 orders have more than one review
- comment title null in 87,656 rows, comment message null in 58,247 rows

**Decision:** Kept the latest review per order (by review_answer_timestamp) so joins to orders do not duplicate rows; dropped the mostly-empty title column, kept the comment message.

## geolocation

- Rows (raw): **1,000,163**
- Nulls per column: none

**Issues found**

- 42 points outside the Brazil box (lat -34.0..6.0, lng -74.0..-34.0)
- 261,831 exact duplicate rows; 19,015 zip prefixes share 1,000,163 points

**Decision:** Dropped out-of-Brazil points, then averaged lat/lng per zip prefix (1,000,121 points left -> 19,010 zip rows; 5 prefixes had only outlier points). 158 distinct customer zip prefixes have no geo match.

## category_translation

- Rows (raw): **71**
- Key `product_category_name`: 71 distinct, 0 duplicated key values
- Nulls per column: none

**Issues found**

- no issues found; 2 product categories are missing from it (see products)

**Decision:** Lookup only: left-joined onto products, not written separately.

## order_lines

- Rows (raw): **112,650**
- Nulls per column: `delivery_days`=2,454, `delay_days`=2,454, `is_late`=2,454, `review_score`=942

**Issues found**

- no join fan-out: rows equal order_items
- delivery_days: 2,454 nulls
- delay_days: 2,454 nulls
- is_late: 2,454 nulls
- review_score: 942 nulls

**Decision:** Derived table: order_items LEFT JOIN orders, customers, sellers, products, reviews (each unique on its key). Null review_score = order never reviewed; null delivery fields = not delivered.

## Row-count check

`order_items` = 112,650 rows and `order_lines` = 112,650 rows (equal, no join fan-out).
