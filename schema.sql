CREATE TABLE IF NOT EXISTS movies (
    movieId INTEGER PRIMARY KEY,
    imdbId TEXT UNIQUE,
    title TEXT,
    release_year INTEGER,
    director TEXT,
    plot TEXT,
    box_office TEXT
);

-- 2. genres table: Stores a list of unique, distinct genres.
-- Uses AUTOINCREMENT for the primary key.
-- UNIQUE constraint on genre_name prevents duplicate genre entries.
CREATE TABLE IF NOT EXISTS genres (
    genreId INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT UNIQUE NOT NULL
);

-- 3. movie_genres table: The mapping table for the many-to-many relationship between movies and genres.
-- Uses a composite PRIMARY KEY to ensure a movie-genre pair is unique.
CREATE TABLE IF NOT EXISTS movie_genres (
    movieId INTEGER NOT NULL,
    genreId INTEGER NOT NULL,
    PRIMARY KEY (movieId, genreId),
    -- Foreign keys link to the respective primary keys and ensure data integrity.
    -- ON DELETE CASCADE ensures related entries are removed if a movie or genre is deleted.
    FOREIGN KEY (movieId) REFERENCES movies (movieId) ON DELETE CASCADE,
    FOREIGN KEY (genreId) REFERENCES genres (genreId) ON DELETE CASCADE
);

-- 4. ratings table: Stores user ratings for movies.
-- The composite primary key (userId, movieId) ensures a user can only rate a movie once.
CREATE TABLE IF NOT EXISTS ratings (
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL,
    rating REAL NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (userId, movieId),
    FOREIGN KEY (movieId) REFERENCES movies (movieId) ON DELETE CASCADE
);
