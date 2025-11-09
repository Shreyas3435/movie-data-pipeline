
.mode column
.headers on


SELECT 
    m.clean_title AS movie_title,
    m.year,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.clean_title, m.year
HAVING COUNT(r.rating) >= 10  
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;



SELECT 
    g.genre_name,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(DISTINCT m.movieId) AS num_movies,
    COUNT(r.rating) AS total_ratings
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN movies m ON mg.movie_id = m.movieId
JOIN ratings r ON m.movieId = r.movieId
GROUP BY g.genre_name
HAVING COUNT(r.rating) >= 100 
ORDER BY avg_rating DESC
LIMIT 5;



SELECT 
    Director,
    COUNT(DISTINCT movieId) AS movie_count,
    GROUP_CONCAT(clean_title, ', ') AS movie_titles
FROM movies
WHERE Director != 'N/A' 
  AND Director IS NOT NULL
  AND Director != ''
GROUP BY Director
ORDER BY movie_count DESC, Director
LIMIT 1;



SELECT 
    m.year AS release_year,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(DISTINCT m.movieId) AS num_movies,
    COUNT(r.rating) AS total_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year DESC;


.print "\n========== BONUS: Top 10 Most Popular Movies =========="

SELECT 
    m.clean_title,
    m.year,
    COUNT(r.rating) AS num_ratings,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.clean_title, m.year
ORDER BY num_ratings DESC
LIMIT 10;

-- Bonus Query 2: Genre distribution by decade
.print "\n========== BONUS: Genre Distribution by Decade =========="

SELECT 
    m.decade,
    g.genre_name,
    COUNT(DISTINCT m.movieId) AS movie_count
FROM movies m
JOIN movie_genres mg ON m.movieId = mg.movie_id
JOIN genres g ON mg.genre_id = g.genre_id
WHERE m.decade IS NOT NULL
GROUP BY m.decade, g.genre_name
ORDER BY m.decade DESC, movie_count DESC
LIMIT 20;


.print "\n========== BONUS: Highest IMDB Rated Movies =========="

SELECT 
    clean_title,
    year,
    Director,
    imdbRating,
    BoxOffice
FROM movies
WHERE imdbRating != 'N/A' 
  AND imdbRating IS NOT NULL
  AND CAST(imdbRating AS REAL) > 0
ORDER BY CAST(imdbRating AS REAL) DESC
LIMIT 10;



.print "\n========== DATA QUALITY SUMMARY =========="


SELECT 'Movies' AS table_name, COUNT(*) AS record_count FROM movies
UNION ALL
SELECT 'Ratings', COUNT(*) FROM ratings
UNION ALL
SELECT 'Genres', COUNT(*) FROM genres
UNION ALL
SELECT 'Movie-Genre Relationships', COUNT(*) FROM movie_genres;


.print "\n========== API ENRICHMENT SUCCESS RATE =========="

SELECT 
    ROUND(100.0 * SUM(CASE WHEN Director != 'N/A' THEN 1 ELSE 0 END) / COUNT(*), 2) AS director_success_rate,
    ROUND(100.0 * SUM(CASE WHEN Plot != 'N/A' THEN 1 ELSE 0 END) / COUNT(*), 2) AS plot_success_rate,
    ROUND(100.0 * SUM(CASE WHEN BoxOffice != 'N/A' THEN 1 ELSE 0 END) / COUNT(*), 2) AS boxoffice_success_rate
FROM movies;