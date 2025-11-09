
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;


CREATE TABLE movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL,                
    clean_title TEXT,                  
    year INTEGER,                      
    decade INTEGER,                 
    genres TEXT,                      
    Director TEXT,                      
    Plot TEXT,                          
    BoxOffice TEXT,                     
    BoxOfficeParsed INTEGER,            
    Runtime TEXT,                      
    Country TEXT,                       
    imdbRating TEXT                     
);


CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT UNIQUE NOT NULL
);


CREATE TABLE movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);


CREATE TABLE ratings (
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL,
    rating REAL NOT NULL CHECK (rating >= 0 AND rating <= 5),
    timestamp INTEGER NOT NULL,
    FOREIGN KEY(movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);


CREATE INDEX idx_movies_year ON movies(year);
CREATE INDEX idx_movies_director ON movies(Director);
CREATE INDEX idx_movies_decade ON movies(decade);
CREATE INDEX idx_ratings_movie ON ratings(movieId);
CREATE INDEX idx_ratings_user ON ratings(userId);
CREATE INDEX idx_movie_genres_movie ON movie_genres(movie_id);
CREATE INDEX idx_movie_genres_genre ON movie_genres(genre_id);

