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
dataset = pd.read_csv('movies_metadata.csv')

# Define the table name
table_name = 'movies'

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
    # Truncate data in each column
    truncated_row = [str(value)[:100] for value in row]  # Truncate to a maximum of 100 characters, modify as needed

    # Construct the parameterized query
    insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(truncated_row))});"
    cursor.execute(insert_query, truncated_row)  # Pass the truncated row values as parameters
conn.commit()

# Close the cursor and database connection
cursor.close()
conn.close()
