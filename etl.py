
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
import time
import re
import logging
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"
OMDB_API_KEY = "e22af311"  
OMDB_URL = "http://www.omdbapi.com/"
DATABASE = "sqlite:///movies.db"


MAX_MOVIES_TO_PROCESS = 100 
API_RATE_LIMIT_DELAY = 0.3 



def fetch_omdb_details(title, year=None):
    """
    Fetch movie details from OMDb API
    Returns: dict with Director, Plot, BoxOffice, Runtime, Country, imdbRating
    """
    try:
        params = {"t": title, "apikey": OMDB_API_KEY, "type": "movie"}
        if year:
            params["y"] = str(int(year))
        
        resp = requests.get(OMDB_URL, params=params, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get("Response") == "True":
                return {
                    "Director": data.get("Director", "N/A"),
                    "Plot": data.get("Plot", "N/A"),
                    "BoxOffice": data.get("BoxOffice", "N/A"),
                    "Runtime": data.get("Runtime", "N/A"),
                    "Country": data.get("Country", "N/A"),
                    "imdbRating": data.get("imdbRating", "N/A")
                }
        
        logger.warning(f"No OMDb data found for: {title} ({year})")
    except Exception as e:
        logger.error(f"API error for {title}: {e}")
    
    return {
        "Director": "N/A",
        "Plot": "N/A",
        "BoxOffice": "N/A",
        "Runtime": "N/A",
        "Country": "N/A",
        "imdbRating": "N/A"
    }


def parse_genres(genres_str):
    """Parse pipe-separated genres into list"""
    if pd.isna(genres_str) or genres_str == '':
        return []
    return [g.strip() for g in genres_str.split("|") if g.strip() and g.strip() != '(no genres listed)']


def parse_box_office(value):
    """Parse box office string to integer"""
    try:
        if value and isinstance(value, str) and value.startswith('$'):
            return int(value.replace('$', '').replace(',', ''))
    except:
        pass
    return None


def extract_year(title):
    """Extract year from movie title format: 'Movie Name (YYYY)'"""
    match = re.search(r'\((\d{4})\)$', str(title))
    return int(match.group(1)) if match else None


def clean_title(title):
    """Remove year from title"""
    return re.sub(r'\s*\(\d{4}\)$', '', str(title)).strip()




def main():
    logger.info("=" * 60)
    logger.info("Starting Movie Data Pipeline")
    logger.info("=" * 60)
    start_time = datetime.now()
    
    try:
        logger.info("PHASE 1: EXTRACTING DATA")
        logger.info("-" * 60)
       
        engine = create_engine(DATABASE)
        logger.info(f"✓ Connected to database: {DATABASE}")
        
       
        logger.info(f"Loading movies from {MOVIES_CSV}...")
        movies = pd.read_csv(MOVIES_CSV)
        logger.info(f"✓ Loaded {len(movies)} movies")
        
        logger.info(f"Loading ratings from {RATINGS_CSV}...")
        ratings = pd.read_csv(RATINGS_CSV)
        logger.info(f"✓ Loaded {len(ratings)} ratings")
       
        if MAX_MOVIES_TO_PROCESS and MAX_MOVIES_TO_PROCESS > 0:
            movies = movies.head(MAX_MOVIES_TO_PROCESS)
            logger.info(f"✓ Limited to {MAX_MOVIES_TO_PROCESS} movies for processing")
        
        
        logger.info("\nPHASE 2: TRANSFORMING DATA")
        logger.info("-" * 60)
        
        
        logger.info("Extracting years and cleaning titles...")
        movies['year'] = movies['title'].apply(extract_year)
        movies['clean_title'] = movies['title'].apply(clean_title)
        
        movies['decade'] = (movies['year'] // 10 * 10).astype('Int64')
        logger.info("✓ Added decade column (Feature Engineering)")
        
      
        movies['genres'] = movies['genres'].fillna('')
        movies['title'] = movies['title'].fillna('Unknown')
        logger.info("✓ Cleaned missing values")
        
        
        movies['genre_list'] = movies['genres'].apply(parse_genres)
        logger.info("✓ Parsed genres")
        
        
        logger.info("\nEnriching data with OMDb API...")
        logger.info(f"Making API calls for {len(movies)} movies...")
        
        omdb_fields = ["Director", "Plot", "BoxOffice", "Runtime", "Country", "imdbRating"]
        for field in omdb_fields:
            movies[field] = None
        
        api_call_count = 0
        for idx, row in movies.iterrows():
            title = row['clean_title']
            year = row['year'] if not pd.isna(row['year']) else None
            
            logger.info(f"  [{idx + 1}/{len(movies)}] Fetching: {title} ({year})")
            
            
            time.sleep(API_RATE_LIMIT_DELAY)
            
            details = fetch_omdb_details(title, year)
            api_call_count += 1
            
            for field in omdb_fields:
                movies.at[idx, field] = details[field]
        
        logger.info(f"✓ Completed {api_call_count} API calls")
        
        
        movies['BoxOfficeParsed'] = movies['BoxOffice'].apply(parse_box_office)
        logger.info("✓ Parsed box office values")
        
        
        logger.info("\nPHASE 3: LOADING DATA INTO DATABASE")
        logger.info("-" * 60)
        
        
        ratings_filtered = ratings[ratings['movieId'].isin(movies['movieId'])]
        logger.info(f"✓ Filtered to {len(ratings_filtered)} relevant ratings")
        
        
        movies_for_db = movies[[
            "movieId", "title", "clean_title", "year", "decade", "genres",
            "Director", "Plot", "BoxOffice", "BoxOfficeParsed", 
            "Runtime", "Country", "imdbRating"
        ]].copy()
        
        ratings_for_db = ratings_filtered[["userId", "movieId", "rating", "timestamp"]].copy()
      
        logger.info("Creating normalized genres table...")
        all_genres = []
        for genre_list in movies['genre_list']:
            all_genres.extend(genre_list)
        
        unique_genres = pd.DataFrame({
            'genre_name': sorted(set(all_genres))
        })
        unique_genres['genre_id'] = range(1, len(unique_genres) + 1)
        
      
        logger.info("Creating movie-genre relationships...")
        movie_genres_data = []
        for idx, row in movies.iterrows():
            movie_id = row['movieId']
            for genre in row['genre_list']:
                genre_id = unique_genres[unique_genres['genre_name'] == genre]['genre_id'].values[0]
                movie_genres_data.append({
                    'movie_id': movie_id,
                    'genre_id': genre_id
                })
        
        movie_genres_df = pd.DataFrame(movie_genres_data)
        
     
        logger.info("Writing to database tables...")
        
        with engine.connect() as conn:
           
            logger.info("  Clearing existing data...")
            conn.execute(text("DROP TABLE IF EXISTS movie_genres"))
            conn.execute(text("DROP TABLE IF EXISTS ratings"))
            conn.execute(text("DROP TABLE IF EXISTS genres"))
            conn.execute(text("DROP TABLE IF EXISTS movies"))
            conn.commit()
            
        
            logger.info("  Creating tables...")
            schema_sql = """
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
                FOREIGN KEY (movie_id) REFERENCES movies(movieId),
                FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
            );
            
            CREATE TABLE ratings (
                userId INTEGER,
                movieId INTEGER,
                rating REAL,
                timestamp INTEGER,
                FOREIGN KEY(movieId) REFERENCES movies(movieId)
            );
            
            CREATE INDEX idx_movies_year ON movies(year);
            CREATE INDEX idx_movies_director ON movies(Director);
            CREATE INDEX idx_ratings_movie ON ratings(movieId);
            """
            
            for statement in schema_sql.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        
        # Insert data
        movies_for_db.to_sql("movies", engine, if_exists='append', index=False)
        logger.info(f"  ✓ Loaded {len(movies_for_db)} movies")
        
        unique_genres[['genre_id', 'genre_name']].to_sql("genres", engine, if_exists='append', index=False)
        logger.info(f"  ✓ Loaded {len(unique_genres)} genres")
        
        movie_genres_df.to_sql("movie_genres", engine, if_exists='append', index=False)
        logger.info(f"  ✓ Loaded {len(movie_genres_df)} movie-genre relationships")
        
        ratings_for_db.to_sql("ratings", engine, if_exists='append', index=False)
        logger.info(f"  ✓ Loaded {len(ratings_for_db)} ratings")
        
        # ========== SUMMARY ==========
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"Total execution time: {duration:.2f} seconds")
        logger.info(f"Movies processed: {len(movies_for_db)}")
        logger.info(f"Ratings loaded: {len(ratings_for_db)}")
        logger.info(f"Genres identified: {len(unique_genres)}")
        logger.info(f"API calls made: {api_call_count}")
        logger.info(f"Database: {DATABASE}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"\n{'=' * 60}")
        logger.error(f"PIPELINE FAILED: {e}")
        logger.error(f"{'=' * 60}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)