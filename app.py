from flask import Flask, render_template, request
import mysql.connector

app = Flask(__name__)

# Configure MySQL connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password@2003",
    database="DATASET"
)


@app.route('/', methods=['GET', 'POST'])
def search_movies():
    if request.method == 'POST':
        original_language = request.form['language']
        vote_average = request.form['rating']
        budget = request.form['budget']

        
        # Create a cursor
        cursor = db.cursor()

        # Build the query based on the selected options
        query = "SELECT title, tagline FROM movies WHERE original_language = %s AND vote_average >= %s AND budget <= %s"
        cursor.execute(query, (original_language, vote_average, budget))
        results = cursor.fetchall()

        # Close the cursor
        cursor.close()

        return render_template('results.html', results=results)

    return render_template('search.html')


if __name__ == '__main__':
    app.run(debug=True)
