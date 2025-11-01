import pandas as pd 

# Read CSV file
df = pd.read_csv("sales_data.csv")

#df = df.fillna({"amount": 0, "State": "Unknown"})
# View top 5 rows
print(df.head(15))

#Removing Duplicates; 
df = df.drop_duplicates()

#Handle missing data: 
