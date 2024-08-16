import requests
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt

# Step 1: Extract Data
print("Starting data extraction...")
url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
response = requests.get(url)
if response.status_code == 200:
    print("Data successfully extracted.")
    open('time_series_covid19_confirmed_global.csv', 'wb').write(response.content)
else:
    print("Failed to download data.")

# Step 2: Transform Data
print("Starting data transformation...")
df = pd.read_csv('time_series_covid19_confirmed_global.csv')
df = df.groupby(['Country/Region']).sum().iloc[:, 4:]  # Aggregating data by country
df = df.reset_index()  # Resetting index to get a flat DataFrame
df_melted = df.melt(id_vars=['Country/Region'], var_name='Date', value_name='Cases')
df_melted['Date'] = pd.to_datetime(df_melted['Date'])
df_melted.columns = ['Country', 'Date', 'Cases']
print("Data transformed successfully. Here's a sample:")
print(df_melted.head())

# Step 3: Load Data into MySQL
print("Connecting to MySQL...")
db_config = {
    'host': 'localhost',
    'user': 'tharaka',
    'password': '456',
    'database': 'covid_data'
}

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    if conn.is_connected():
        print("Connected to MySQL successfully.")

    for index, row in df_melted.iterrows():
        try:
            cursor.execute("""
                INSERT INTO covid_cases (Country, Date, Cases)
                VALUES (%s, %s, %s)
            """, (row['Country'], row['Date'].strftime('%Y-%m-%d'), row['Cases']))
        except mysql.connector.Error as err:
            print(f"Failed to insert data: {err}")

    conn.commit()
    print("Data loaded into MySQL successfully.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    cursor.close()
    conn.close()

# Step 4: Visualize Data
print("Visualizing data...")
conn = mysql.connector.connect(**db_config)
query = 'SELECT * FROM covid_cases WHERE Country = %s'
country = 'United Kingdom'
df = pd.read_sql(query, conn, params=(country,))
conn.close()

# Plot data
plt.figure(figsize=(12, 6))
plt.plot(df['Date'], df['Cases'], marker='o')
plt.title(f'COVID-19 Cases in {country}')
plt.xlabel('Date')
plt.ylabel('Number of Cases')
plt.grid(True)
plt.show()

print("ETL Pipeline completed successfully!")
