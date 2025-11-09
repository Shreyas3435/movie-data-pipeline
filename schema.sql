-- ============================================================
-- Movie Data Pipeline Database Schema
-- Enhanced version with normalized design
-- ============================================================

-- Drop existing tables if they exist (for idempotency)
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;

-- ============================================================
-- Movies Table: Core movie information + OMDb API enrichment
-- ============================================================
CREATE TABLE movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL,                -- Original title with year
    clean_title TEXT,                   -- Title without year for cleaner display
    year INTEGER,                       -- Release year
    decade INTEGER,                     -- Feature Engineering: decade of release
    genres TEXT,                        -- Original pipe-separated genres string
    Director TEXT,                      -- Director from OMDb API
    Plot TEXT,                          -- Plot summary from OMDb API
    BoxOffice TEXT,                     -- Box office earnings (raw string)
    BoxOfficeParsed INTEGER,            -- Box office as integer for calculations
    Runtime TEXT,                       -- Runtime from OMDb API
    Country TEXT,                       -- Country from OMDb API
    imdbRating TEXT                     -- IMDB rating from OMDb API
);

-- ============================================================
-- Genres Table: Normalized unique genre names
-- ============================================================
CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT UNIQUE NOT NULL
);

-- ============================================================
-- Movie_Genres Table: Many-to-many relationship
-- (A movie can have multiple genres, a genre can have multiple movies)
-- ============================================================
CREATE TABLE movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

-- ============================================================
-- Ratings Table: User ratings for movies
-- ============================================================
CREATE TABLE ratings (
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL,
    rating REAL NOT NULL CHECK (rating >= 0 AND rating <= 5),
    timestamp INTEGER NOT NULL,
    FOREIGN KEY(movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- ============================================================
-- Indexes for Query Performance
-- ============================================================
CREATE INDEX idx_movies_year ON movies(year);
CREATE INDEX idx_movies_director ON movies(Director);
CREATE INDEX idx_movies_decade ON movies(decade);
CREATE INDEX idx_ratings_movie ON ratings(movieId);
CREATE INDEX idx_ratings_user ON ratings(userId);
CREATE INDEX idx_movie_genres_movie ON movie_genres(movie_id);
CREATE INDEX idx_movie_genres_genre ON movie_genres(genre_id);

-- ============================================================
-- Schema Design Notes:
-- 1. Movies table contains both CSV data and OMDb API enrichment
-- 2. Genres normalized to separate table to avoid data redundancy
-- 3. Movie_genres implements many-to-many relationship
-- 4. Ratings table links users to movies with timestamps
-- 5. Indexes on frequently queried columns improve performance
-- 6. Foreign keys ensure referential integrity
-- 7. Decade column enables temporal analysis (feature engineering)
-- ============================================================