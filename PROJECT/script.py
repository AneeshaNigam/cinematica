import csv
import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="postgres",
    user="postgres",
    password="password@2003"
)

# Open the dataset file
with open('c:/Users/anees/Desktop/PROJECT/credits.csv', 'r') as file:
    # Create a cursor to execute SQL queries
    cursor = conn.cursor()

    # Create the table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS dataset_table (
            cast_ varchar,
            crew varchar,
            id SERIAL PRIMARY KEY
        )
    '''
    cursor.execute(create_table_query)
    conn.commit()

    # Read the dataset and insert data into the table
    csv_reader = csv.reader(file)
    next(csv_reader)  # Skip the header row if it exists

    for row in csv_reader:
        cast_ = row[0]
        crew = row[1]
        id = row[2]

        insert_query = '''
            INSERT INTO dataset_table (cast_, crew, id)
            VALUES (%s, %s, %s)
        '''
        cursor.execute(insert_query, (cast_, crew, id))

    conn.commit()

# Close the database connection
cursor.close()
conn.close()
