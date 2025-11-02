import pandas as pd 

df = pd.read_csv("sales_data.csv")

#Clean 
df = df.drop_duplicates()
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df['quantity'] = df['quantity'].fillna(0)
df['price_usd'] = (
    df['price_usd']
    .astype(str)
    .str.replace('USD', '')
    .astype(float)
)

df['total_sales'] = df['quantity'] * df['price_usd']

df['region'] = df['region'].str.title()

print(df.head(15))
df.to_csv("cleaned_sales_data.csv", index=False)

