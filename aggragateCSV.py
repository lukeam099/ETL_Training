import pandas as pd

df = pd.read_csv("cleaned_sales_data.csv")
df['total_sales'] = df['quantity'] * df['price_usd']

ny_sales = df[df['store_id'] == 'NY01']

ny_summary = ny_sales.groupby('product').agg({
    'total_sales': 'sum', 
    'quantity': 'sum'
    }).reset_index()

print(ny_summary)