import pandas as pd
import sqlite3
import requests
import os
import re
from tqdm import tqdm
from sqlalchemy import create_engine

DATABASE_NAME = 'movie_pipeline.db'
OMDB_API_KEY = os.environ.get('OMDB_API_KEY')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, 'data')

def setup_database():
    print("Setting up database and tables...")
    try:
        # It connects to the SqLite database. If the file is not there, it will create one
        conn = sqlite3.connect(DATABASE_NAME) 
        cursor = conn.cursor()
        
        # Read & execute the schema.sql file 
        with open('C:/Users/SASI/Downloads/project_1/movie_data_pipeline/venv/Scripts/schema.sql', 'r') as f: 
            cursor.executescript(f.read())        
        conn.commit()
        conn.close()
        print("Database setup complete.")
    except Exception as e:
        print(f"Error during database setup: {e}")
        # Stop the program if setup fails, because the next steps need the table.
        exit(1)
def extract_csv():
    """Reads movies.csv, ratings.csv, and links.csv into DataFrames."""
    print("Extracting data from CSV files...")
    
    try:
        print(DATA_PATH)
        # Load the CSVs files
        df_movies = pd.read_csv('C:/Users/SASI/Downloads/project_1/movie_data_pipeline/venv/Scripts/movies.csv')
        df_ratings = pd.read_csv('C:/Users/SASI/Downloads/project_1/movie_data_pipeline/venv/Scripts/ratings.csv')
        df_links = pd.read_csv('C:/Users/SASI/Downloads/project_1/movie_data_pipeline/venv/Scripts/links.csv')
        
        # Combine movies and links to get the IMDB ID, which is needed to call the OMDb API.
        df_movies_imdb = pd.merge(
            df_movies, 
            df_links[['movieId', 'imdbId']], 
            on='movieId', 
            how='inner'
        )
        
        print(f"Extracted {len(df_movies_imdb)} movies and {len(df_ratings)} ratings.")
        return df_movies_imdb, df_ratings
    except FileNotFoundError as e:
        print(f"Error: One or more CSV files not found. Ensure they are in the 'data' folder. Details: {e}")
        exit(1)
    except Exception as e:
        print(f"Error during CSV extraction: {e}")
        exit(1)
def enrich_data(df_movies_imdb):
    
    print("\nEnriching data via OMDb API calls...")
    
    if not OMDB_API_KEY:
        print("Error: OMDB_API_KEY environment variable not set. Skipping API enrichment.")
        # If the API key is missing, return the original DataFrame.
        return df_movies_imdb
        
    omdb_details = [] # List to store data from the API 
    
    # Go through each row in the DataFrame and make API calls. Use tqdm to show a progress bar 
    for index, row in tqdm(
        df_movies_imdb.iterrows(), 
        total=len(df_movies_imdb),
        desc="OMDb API Calls"
    ):
        # Format the IMDB ID by adding 'tt' in front and filling zeros to make 7 digits
        imdb_id = 'tt' + str(row['imdbId']).zfill(7)
        
        # Construct the API URL
        url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&i={imdb_id}"
        
        try:
            response = requests.get(url).json() 
            
            # Check the API response was successful
            if response.get('Response') == 'True': 
                # Extract required fields from the API response 
                omdb_details.append({
                    'movieId': row['movieId'], 
                    'director': response.get('Director', 'N/A'),
                    'plot': response.get('Plot', 'N/A'),
                    'box_office': response.get('BoxOffice', 'N/A')
                })
            else:
                # Handle movie not found (e.g., set fields to 'N/A')
                omdb_details.append({
                    'movieId': row['movieId'], 
                    'director': 'N/A', 
                    'plot': 'N/A',
                    'box_office': 'N/A'
                })
                
        except requests.exceptions.RequestException as e:
            # Handle network or API error
            print(f"\nAPI Error for {imdb_id}: {e}")
            omdb_details.append({
                'movieId': row['movieId'], 
                'director': 'N/A', 
                'plot': 'N/A', 
                'box_office': 'N/A'
            })

    # Change the list of dictionaries into a DataFrame.
    df_omdb = pd.DataFrame(omdb_details)
    
    # Combine the enriched data with the original movie DataFrame
    df_enriched = pd.merge(df_movies_imdb, df_omdb, on='movieId', how='left') 
    
    return df_enriched
def transform_data(df_movies_enriched, df_ratings):
    """Performs cleaning, feature engineering, and genre flattening.""" 
    print("\nTransforming and cleaning data...")
    
    # --- 1. Year Extraction and Title Cleaning (Feature Engineering 1)
    # Extract the four-digit year from the title string (e.g., "Title (2000)") [cite: 102]
    df_movies_enriched['release_year'] = df_movies_enriched['title'].str.extract(r'\((\d{4})\)').astype('Int64', errors='ignore')    
    # Clean the title by removing the year part and surrounding spaces [cite: 103]
    df_movies_enriched['title'] = df_movies_enriched['title'].str.replace(r'\s\(\d{4}\)\$', "", regex=True) 

    # --- 2. Genre Flattening (Most Complex Transformation)
    
    # Split the pipe-separated genres string and explode to create one row per movie-genre combination [cite: 107]
    # 'genres' column is pipe-separated in MovieLens, NOT slash-separated as noted in source, so we'll adjust the split.
    df_genres_flat = df_movies_enriched[['movieId', 'genres']].copy()
    df_genres_flat['genre_name'] = df_genres_flat['genres'].str.split('|')
    df_genres_flat = df_genres_flat.explode('genre_name')
    
    # Create the unique genres table (df_genres)
    df_genres = df_genres_flat[['genre_name']].drop_duplicates().reset_index(drop=True)
    # Assign a unique integer ID to each genre (Simulating AUTOINCREMENT)
    df_genres['genreId'] = df_genres.index + 1
    
    # Create the movie_genres mapping table (df_movie_genres) 
    # Merge the flat table with df_genres to get the genreId
    df_movie_genres = pd.merge(
        df_genres_flat[['movieId', 'genre_name']],
        df_genres,
        on='genre_name',
        how='left'
    )[['movieId', 'genreId']].drop_duplicates()

    # --- 3. Final DataFrames for Loading
    # Final Movies DataFrame (selecting only columns for the movies table)
    df_final_movies = df_movies_enriched[[
        'movieId', 'imdbId', 'title', 'release_year', 'director', 'plot', 'box_office'
    ]]
    
    # Final Ratings DataFrame 
    df_final_ratings = df_ratings[['userId', 'movieId', 'rating', 'timestamp']] 
    print("Transformation complete. Ready to load.")
    return df_final_movies, df_final_ratings, df_genres, df_movie_genres
def load_data(df_final_movies, df_final_ratings, df_genres, df_movie_genres):
    """
    Loads cleaned data into the SQLite tables, handling null values (pd.NA) 
    in-line with the data preparation for each table.
    """
    print("\nLoading data into the SQLite database...")

    # Set up the connection.
    conn = sqlite3.connect(DATABASE_NAME) 
    cursor = conn.cursor() 
    
    # ----------------------------------------------------------------------
    # 1. Load genres table (Idempotent: INSERT OR IGNORE)
    # ----------------------------------------------------------------------
    # Make sure all pd.NA values are changed to None before making the list.
    df_genres_clean = df_genres.copy()
    for col in df_genres_clean.columns:
    # Use pd.isna().any() to correctly check for the presence of any null values, including pd.NA
        if df_genres_clean[col].isna().any():
            df_genres_clean[col] = df_genres_clean[col].replace({pd.NA: None})

    genre_data = df_genres_clean[['genreId', 'genre_name']].values.tolist()
    
    cursor.executemany("""
        INSERT OR IGNORE INTO genres (genreId, genre_name) 
        VALUES (?, ?)
        """, genre_data)
        
    # ----------------------------------------------------------------------
    # 2. Load movies table (Idempotent: INSERT OR IGNORE)
    # ----------------------------------------------------------------------
    # CRITICAL: Handle NAType error fix directly on the required columns (like release_year).
    df_movies_clean = df_final_movies.copy()
    
    # Apply the specific fix for nullable integer columns (like release_year)
    if 'release_year' in df_movies_clean.columns:
        df_movies_clean['release_year'] = df_movies_clean['release_year'].replace({pd.NA: None})
        
    # Also clean director, plot, box_office which might have pd.NA if OMDb returned a column but data was missing/null
    for col in ['director', 'plot', 'box_office']:
        if col in df_movies_clean.columns:
            df_movies_clean[col] = df_movies_clean[col].replace({pd.NA: None})
    
    movie_data = df_movies_clean[[
        'movieId', 'imdbId', 'title', 'release_year', 'director', 'plot', 'box_office'
    ]].values.tolist() 
    
    cursor.executemany("""
        INSERT OR IGNORE INTO movies (movieId, imdbId, title, release_year, director, plot, box_office)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, movie_data)

    # ----------------------------------------------------------------------
    # 3. Load movie_genres mapping table (Transactional: DELETE and INSERT)
    # ----------------------------------------------------------------------
    df_movie_genres_clean = df_movie_genres.copy()
    cursor.execute("DELETE FROM movie_genres")
    # Correction applied here:
    for col in df_movie_genres_clean.columns:
        # Check for the existence of any null value using the vectorized isna().any()
        if df_movie_genres_clean[col].isna().any(): 
            # If nulls exist, replace pd.NA with None
            df_movie_genres_clean[col] = df_movie_genres_clean[col].replace({pd.NA: None})
        
    movie_genre_data = df_movie_genres_clean[['movieId', 'genreId']].values.tolist()

    cursor.executemany("INSERT INTO movie_genres (movieId, genreId) VALUES (?, ?)", movie_genre_data)
    # ----------------------------------------------------------------------
    # 4. Load ratings table (Transactional: DELETE and INSERT)
    # ----------------------------------------------------------------------
    df_ratings_clean = df_final_ratings.copy()
    cursor.execute("DELETE FROM ratings")
    
    # Cleaning the ratings data (mostly important for timestamp if using nullable types)
    for col in df_ratings_clean.columns:
        if df_ratings_clean[col].isna().any():
            df_ratings_clean[col] = df_ratings_clean[col].replace({pd.NA: None})
            
    rating_data = df_ratings_clean[['userId', 'movieId', 'rating', 'timestamp']].values.tolist()
    
    cursor.executemany(
        "INSERT INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)", 
        rating_data
    )

    # Finalize and close
    conn.commit()
    conn.close()
    print("Data loading complete. Database is updated.")
def run_pipeline():
    """Calls all the ETL functions in sequence.""" 
    
    # 1. Setup Database
    setup_database() 
    
    # 2. Extract Data
    df_movies_imdb, df_ratings = extract_csv() 
    
    # 3. Add more information to the data
    df_movies_enriched = enrich_data(df_movies_imdb) 
    
    # 4. Transform Data
    df_final_movies, df_final_ratings, df_genres, df_movie_genres = transform_data(df_movies_enriched, df_ratings)
    
    # 5. Load Data
    load_data(df_final_movies, df_final_ratings, df_genres, df_movie_genres)
    
    print("\n*** ETL Pipeline Run Successfully! ***")


if __name__ == "__main__":
    run_pipeline()