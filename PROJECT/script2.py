import pandas as pd
import mysql.connector

# Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password@2003",
    database="DATASET"
)

# Open the dataset file using pandas
dataset = pd.read_csv('credits.csv')

# Define the table name
table_name = 'credits'

# Create a cursor to execute SQL queries
cursor = conn.cursor()

# Create the table
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for column in dataset.columns:
    column_name = column.replace(' ', '_').lower()  # Modify column names if necessary
    column_type = 'LONGTEXT'  # Modify data types if necessary
    create_table_query += f"{column_name} {column_type}, "
create_table_query = create_table_query.rstrip(', ') + ");"
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
for row in dataset.itertuples(index=False):
    # Construct the parameterized query
    insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))});"
    cursor.execute(insert_query, row)  # Pass the row values as parameters
conn.commit()


# Close the cursor and database connection
cursor.close()
conn.close()
