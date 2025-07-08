import pandas as pd
import numpy as np
import os
from datetime import datetime

RAW_DIR = os.path.join(os.path.dirname(__file__), '../datalake/raw')
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '../datalake/processed')
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Load raw data
df_products = pd.read_csv(os.path.join(RAW_DIR, 'products.csv'))
df_aisles = pd.read_csv(os.path.join(RAW_DIR, 'aisles.csv'))
df_departments = pd.read_csv(os.path.join(RAW_DIR, 'departments.csv'))
df_orders = pd.read_csv(os.path.join(RAW_DIR, 'orders.csv'))
df_date = pd.read_csv(os.path.join(RAW_DIR, 'dim_date.csv'))
df_prior = pd.read_csv(os.path.join(RAW_DIR, 'order_products__prior.csv'))
df_train = pd.read_csv(os.path.join(RAW_DIR, 'order_products__train.csv'))

# 1. Enrich dim_product
# Synthesize unit price for each product
np.random.seed(42)
df_products['unit_price'] = np.round(np.random.uniform(1, 20, size=len(df_products)), 2)
# Join aisle and department names
df_products = df_products.merge(df_aisles, on='aisle_id', how='left')
df_products = df_products.merge(df_departments, on='department_id', how='left')
# Calculate product_order_count and product_reorder_rate
order_counts = pd.concat([df_prior, df_train]).groupby('product_id').size().rename('product_order_count')
reorder_counts = pd.concat([df_prior, df_train])[['product_id', 'reordered']].groupby('product_id').mean().rename({'reordered': 'product_reorder_rate'}, axis=1)
df_products = df_products.join(order_counts, on='product_id')
df_products = df_products.join(reorder_counts, on='product_id')
df_products['product_order_count'] = df_products['product_order_count'].fillna(0).astype(int)
df_products['product_reorder_rate'] = df_products['product_reorder_rate'].fillna(0)
df_products.to_csv(os.path.join(PROCESSED_DIR, 'dim_product.csv'), index=False)

# 2. Enrich dim_aisle
# department_count: number of unique departments in each aisle
department_count = df_products.groupby('aisle_id')['department_id'].nunique().rename('department_count')
df_aisles = df_aisles.join(department_count, on='aisle_id')
df_aisles['department_count'] = df_aisles['department_count'].fillna(0).astype(int)
df_aisles.to_csv(os.path.join(PROCESSED_DIR, 'dim_aisle.csv'), index=False)

# 3. Enrich dim_department
# product_count: number of products in each department
product_count = df_products.groupby('department_id')['product_id'].nunique().rename('product_count')
df_departments = df_departments.join(product_count, on='department_id')
df_departments['product_count'] = df_departments['product_count'].fillna(0).astype(int)
df_departments.to_csv(os.path.join(PROCESSED_DIR, 'dim_department.csv'), index=False)

# 4. Enrich dim_user
user_orders = df_orders.groupby('user_id').agg(
    total_orders=('order_number', 'max'),
    avg_days_between_orders=('days_since_prior_order', 'mean'),
)
# Get first and last order dates for each user
order_dates = df_orders.sort_values(['user_id', 'order_number'])[['user_id', 'order_number']].copy()
order_dates = order_dates.merge(df_date, left_on='order_number', right_index=True, how='left')
first_order = df_orders[df_orders['order_number'] == 1][['user_id']].copy()
first_order = first_order.merge(df_orders[['user_id', 'order_number']], on='user_id')
first_order = first_order.merge(df_date, left_on='order_number', right_index=True, how='left')
last_order = df_orders.groupby('user_id')['order_number'].max().reset_index()
last_order = last_order.merge(df_orders, on=['user_id', 'order_number'])
last_order = last_order.merge(df_date, left_on='order_number', right_index=True, how='left')
df_users = user_orders.reset_index()
df_users['first_order_date'] = np.nan  # Placeholder
df_users['last_order_date'] = np.nan   # Placeholder
df_users.to_csv(os.path.join(PROCESSED_DIR, 'dim_user.csv'), index=False)

# 5. dim_date is already synthesized and enriched, just copy to processed
df_date.to_csv(os.path.join(PROCESSED_DIR, 'dim_date.csv'), index=False)

# 6. Build fact_order_products
# Concatenate prior and train order products
fact_orders = pd.concat([df_prior, df_train])

# Join with orders_with_dates to get user_id, order_number, and order_date
orders_with_dates_path = os.path.join(RAW_DIR, 'orders_with_dates.csv')
df_orders = pd.read_csv(orders_with_dates_path)
fact_orders = fact_orders.merge(
    df_orders[['order_id', 'user_id', 'order_number', 'order_date']],
    on='order_id',
    how='left'
)

# Join with date dimension to get date_id and other date fields
fact_orders = fact_orders.merge(
    df_date,
    left_on='order_date',
    right_on='date',
    how='left'
)

# Join with product to get unit_price
fact_orders = fact_orders.merge(df_products[['product_id', 'unit_price']], on='product_id', how='left')

# Synthesize sales_amount (unit_price * quantity, quantity=1)
fact_orders['sales_amount'] = fact_orders['unit_price']

# Save to processed
fact_orders.to_csv(os.path.join(PROCESSED_DIR, 'fact_order_products.csv'), index=False)

print('Enriched dimension tables and fact table have been saved to datalake/processed/') 