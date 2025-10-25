# Movie_OMDB-project
Built a foundational ETL pipeline using Python, Pandas, and SQLite. Goal: Turn messy MovieLens files and data from the OMDb API into a clean, searchable database. Process: Extract data , transform genres and years , and load into normalized tables. The pipeline demonstrates data cleansing, API integration, and database structuring essentials.
This project is aimed to provide an system to gather, clean and organize movies data into a proper database.

**Prerequisites:**

1. Python3
2. SQLite
3. Visual Studio Code
4. DB Browser. 

**The workflow:**

The project begins with generating our API key using this link: http://www.omdbapi.com/.

We can create the API by using the link and generating an API key for free. The API key will be shared by mail to us.

After generating the API key, we need to get the required movies data to get started with the project.

Access the data from the link https://grouplens.org/datasets/movielens/latest/, download the zip file which has 'small' word in it.

In the zip file export all the csv files and move them into your 'venv' folder for ease of usage.

**Execution:**

Import all the required libraries for the project.

Required Libraries:

1. Pandas
2. sqlite3
3. requests
4. tqdm
5. sqlalchemy
6. re

After importing the necessary libraries clone the repository.

**Note:** After generating the API key it must be set as environment variable in the venv environment so that the API can be called from the device.

Use the link to set, $env:OMDB\_API\_KEY="Your API Key".

**Note:** Make sure to use the right encoding for files while handling the csv files, the file path should be correct and encoding is important. If normal path is giving error use the '/' in setting the file path.

**Database Design:**

1. Database Design Rationale The database uses a normalized structure (4 tables: movies, ratings, genres, and movie\_genres). We separated genres to avoid repeating the same genre names thousands of times, making the database cleaner and more efficient.
2. Key Problem SolvedAPI Calls: We had to use the links.csv file to get the reliable IMDB ID for each movie. We used this unique ID instead of the movie title when calling the OMDb API to ensure we found the correct movie every time. 
3. Missing Data: If the API failed or a movie had no box office data, we set the missing field to 'N/A' to prevent the pipeline from crashing.
4. Data Types: We fixed errors during the loading step by explicitly converting Pandas' special null values (pd.NA) into Python None, which the SQLite database understands as an empty slot (NULL).
5. Avoiding Duplicates: We used INSERT OR IGNORE for the movies and genres tables so you can run the script multiple times without creating duplicate entries.

**Query Execution:**

After successfully creating the pipeline we can check the database(We used SQLite) for checking the Database structure and to execute few queries.



**Result:**

The project successfully performs the ETL actions on the movies data and gives us clean enriched data which can be used for data analysis.







