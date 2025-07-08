import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

RAW_DIR = os.path.join(os.path.dirname(__file__), '../datalake/raw')
ORDERS_CSV = os.path.join(RAW_DIR, 'orders.csv')
DATE_DIM_CSV = os.path.join(RAW_DIR, 'dim_date.csv')

# Load orders
df_orders = pd.read_csv(ORDERS_CSV)

# Sort by user and order_number
orders_sorted = df_orders.sort_values(['user_id', 'order_number'])

# Assign a random start date for each user (between 2015-01-01 and 2016-12-31)
user_start_dates = {
    user_id: datetime.strptime('2015-01-01', '%Y-%m-%d') + timedelta(days=int(np.random.randint(0, 730)))
    for user_id in orders_sorted['user_id'].unique()
}

# Calculate actual order dates
def compute_order_dates(df):
    order_dates = []
    for user_id, group in df.groupby('user_id'):
        group = group.sort_values('order_number')
        last_date = user_start_dates[user_id]
        for idx, row in group.iterrows():
            if pd.isnull(row['days_since_prior_order']) or row['order_number'] == 1:
                order_date = last_date
            else:
                order_date = last_date + timedelta(days=float(row['days_since_prior_order']))
            order_dates.append(order_date)
            last_date = order_date
    return order_dates

orders_sorted['order_date'] = compute_order_dates(orders_sorted)

# Build date dimension BEFORE converting order_date to string
date_dim = orders_sorted[['order_date']].drop_duplicates().copy()
date_dim['date_id'] = date_dim['order_date'].dt.strftime('%Y%m%d')
date_dim['date'] = date_dim['order_date'].dt.strftime('%Y-%m-%d')
date_dim['year'] = date_dim['order_date'].dt.year
date_dim['month'] = date_dim['order_date'].dt.month
date_dim['day'] = date_dim['order_date'].dt.day
date_dim['weekday'] = date_dim['order_date'].dt.day_name()
date_dim['is_weekend'] = date_dim['order_date'].dt.weekday >= 5
date_dim = date_dim[['date_id', 'date', 'year', 'month', 'day', 'weekday', 'is_weekend']]
date_dim.to_csv(DATE_DIM_CSV, index=False)
print(f"Synthesized {len(date_dim)} unique dates and saved to {DATE_DIM_CSV}")

# Now convert order_date to string for CSV output
orders_sorted['order_date'] = orders_sorted['order_date'].dt.strftime('%Y-%m-%d')
orders_with_dates_csv = os.path.join(RAW_DIR, 'orders_with_dates.csv')
orders_sorted.to_csv(orders_with_dates_csv, index=False)
print(f"Saved orders with order_date to {orders_with_dates_csv}") 