-- 1. Query: Top 10 Highest-Rated Movies (with at least 50 ratings)

-- Calculate the average rating for each movie and remove movies with too few ratings.
SELECT
    m.title,
    CAST(AVG(r.rating) AS REAL) AS average_rating,
    COUNT(r.rating) AS rating_count
FROM
    movies m
JOIN
    ratings r ON m.movieId = r.movieId
GROUP BY
    m.movieId, m.title
HAVING
    rating_count >= 50  -- Setting  minimum number of ratings to 50 So the average can be Accurate.
ORDER BY
    average_rating DESC
LIMIT 10;

-- 2. Query: Top 5 Most Popular Genres by Total Movie Count

-- Determines which genres have the most titles associated with them.
SELECT
    g.genre_name,
    COUNT(mg.movieId) AS movie_count
FROM
    genres g
JOIN
    movie_genres mg ON g.genreId = mg.genreId
GROUP BY
    g.genre_name
ORDER BY
    movie_count DESC
LIMIT 5;

-- 3. Query: Top 5 Directors by Average Rating (with at least 10 ratings across their films)

-- Identifies the directors whose movies collectively receive the highest average ratings,
-- again applying a minimum count for reliability.
SELECT
    m.director,
    CAST(AVG(r.rating) AS REAL) AS average_director_rating,
    COUNT(r.rating) AS total_ratings_count
FROM
    movies m
JOIN
    ratings r ON m.movieId = r.movieId
WHERE
    m.director IS NOT NULL AND m.director != 'N/A' -- Excluding missing directors
GROUP BY
    m.director
HAVING
    total_ratings_count >= 10 -- Minimum total ratings across all their movies
ORDER BY
    average_director_rating DESC
LIMIT 5;


-- 4. Query: Movies Released in 1995 with an Average Rating > 4.0

-- Filters movies by both release year and quality (average rating) to find highly-rated classics.
SELECT
    m.title,
    m.release_year,
    CAST(AVG(r.rating) AS REAL) AS average_rating
FROM
    movies m
JOIN
    ratings r ON m.movieId = r.movieId
WHERE
    m.release_year = 1995
GROUP BY
    m.movieId, m.title, m.release_year
HAVING
    average_rating > 4.0
ORDER BY
    average_rating DESC;