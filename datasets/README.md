# Datasets (not stored in Git)

The CSV files are too large for GitHub, so download them yourself.

## Olist Brazilian E-Commerce (required)
1. Download from Kaggle (a free account is needed): https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Unzip all 9 CSV files into `datasets/olist-dataset/`, so you have:

```text
datasets/olist-dataset/
  olist_customers_dataset.csv
  olist_geolocation_dataset.csv
  olist_order_items_dataset.csv
  olist_order_payments_dataset.csv
  olist_order_reviews_dataset.csv
  olist_orders_dataset.csv
  olist_products_dataset.csv
  olist_sellers_dataset.csv
  product_category_name_translation.csv
```

If you use the Kaggle CLI (`pip install kaggle`, with an API token in `~/.kaggle/kaggle.json`):

```bash
kaggle datasets download -d olistbr/brazilian-ecommerce -p datasets/olist-dataset --unzip
```

License: CC BY-NC-SA 4.0 (Olist). Credit: "Brazilian E-Commerce Public Dataset by Olist", Kaggle.
