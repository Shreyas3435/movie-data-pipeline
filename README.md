Movie Data Pipeline 🎬
A production-ready ETL (Extract, Transform, Load) pipeline that ingests movie data from CSV files, enriches it with the OMDb API, and loads it into a SQLite database for analytics.

📋 Table of Contents
Project Overview
Features
Project Structure
Prerequisites
Setup Instructions
Running the Pipeline
Database Schema
Analytical Queries
Design Decisions
Challenges & Solutions
Production Improvements
Troubleshooting
🎯 Project Overview
This project implements a complete data engineering solution for processing movie data. It combines:

MovieLens CSV data (movies.csv, ratings.csv)
OMDb API enrichment (Director, Plot, Box Office, Runtime, Country, IMDB Rating)
SQLite database for analytical queries
Key Objectives
✅ Extract data from multiple sources (CSV + API)
✅ Transform and clean data with feature engineering
✅ Load into normalized relational database
✅ Answer analytical business questions
✅ Handle errors gracefully
✅ Ensure idempotent pipeline execution

✨ Features
Multi-source Integration: Combines local CSV files with REST API data
Data Enrichment: Fetches 6+ additional fields per movie from OMDb
Feature Engineering: Adds decade categorization for temporal analysis
Normalized Schema: Proper many-to-many relationship for genres
Error Handling: Gracefully handles missing data and API failures
Idempotent Design: Safe to run multiple times without duplicates
Comprehensive Logging: Both file and console logging for monitoring
Rate Limiting: Respects API limits with configurable delays
Statistical Filters: Uses minimum rating thresholds for meaningful insights
📁 Project Structure
movie-data-pipeline/
├── etl.py                 # Main ETL pipeline script
├── schema.sql             # Database schema definition
├── queries.sql            # Analytical SQL queries
├── README.md              # This file
├── movies.csv             # MovieLens movies data (you provide)
├── ratings.csv            # MovieLens ratings data (you provide)
├── movies.db              # SQLite database (created by pipeline)
└── etl_pipeline.log       # Execution log (created by pipeline)
📋 Prerequisites
Required Software
Python 3.7+ (Python 3.8 or higher recommended)
VS Code (or any text editor)
Internet connection (for OMDb API calls)
Required Python Libraries
bash
pandas==2.1.4
requests==2.31.0
sqlalchemy==2.0.23
🚀 Setup Instructions
Step 1: Download MovieLens Dataset
Visit: https://grouplens.org/datasets/movielens/latest/
Download ml-latest-small.zip (small dataset, ~1MB)
Extract the ZIP file
Copy movies.csv and ratings.csv to your project folder
Your folder should look like:

movie-data-pipeline/
├── etl.py
├── schema.sql
├── queries.sql
├── README.md
├── movies.csv          ← Place here
└── ratings.csv         ← Place here
Step 2: Get OMDb API Key
Visit: http://www.omdbapi.com/apikey.aspx
Select FREE plan (1,000 daily requests)
Enter your email and verify
Copy your API key
Step 3: Configure API Key
Option A: Edit etl.py directly (Easiest)

Open etl.py in VS Code
Find line 24: OMDB_API_KEY = "e22af311"
Replace with your API key: OMDB_API_KEY = "YOUR_KEY_HERE"
Option B: Use environment variable

bash
# Windows
set OMDB_API_KEY=your_key_here

# Mac/Linux
export OMDB_API_KEY=your_key_here
Step 4: Install Dependencies
Open Terminal in VS Code (Terminal > New Terminal) and run:

bash
# Install required packages
pip install pandas requests sqlalchemy

# Or if you have a requirements.txt:
pip install -r requirements.txt
Note for Windows Users: If you get an error, try:

bash
python -m pip install pandas requests sqlalchemy
🎬 Running the Pipeline
Quick Start (Process 100 Movies)
The default configuration processes 100 movies for quick testing:

bash
python etl.py
Expected Output:

============================================================
Starting Movie Data Pipeline
============================================================
INFO - PHASE 1: EXTRACTING DATA
------------------------------------------------------------
INFO - ✓ Connected to database: sqlite:///movies.db
INFO - Loading movies from movies.csv...
INFO - ✓ Loaded 9742 movies
INFO - ✓ Limited to 100 movies for processing
INFO - Loading ratings from ratings.csv...
INFO - ✓ Loaded 100836 ratings
...
INFO - ✓ Completed 100 API calls
...
============================================================
PIPELINE COMPLETED SUCCESSFULLY!
============================================================
Total execution time: 127.45 seconds
Movies processed: 100
Ratings loaded: 15234
Genres identified: 18
API calls made: 100
Database: sqlite:///movies.db
============================================================
Process ALL Movies (Full Dataset)
To process all movies, edit etl.py:

python
# Line 26-27: Change this:
MAX_MOVIES_TO_PROCESS = 100

# To this:
MAX_MOVIES_TO_PROCESS = 0  # 0 means process all movies
⚠️ Warning: Processing all ~9,700 movies will take approximately 1 hour due to API rate limiting.

Adjust API Rate Limiting
If you encounter rate limit errors, increase the delay:

python
# Line 28: Change from 0.3 to higher value
API_RATE_LIMIT_DELAY = 0.5  # Increase delay between API calls
🗄️ Database Schema
Entity-Relationship Diagram
┌──────────────┐       ┌─────────────────┐       ┌──────────────┐
│    movies    │       │  movie_genres   │       │    genres    │
├──────────────┤       ├─────────────────┤       ├──────────────┤
│ movieId (PK) │───┐   │ movie_id (FK)   │   ┌───│ genre_id(PK) │
│ title        │   └───│ genre_id (FK)   │───┘   │ genre_name   │
│ year         │       └─────────────────┘       └──────────────┘
│ decade       │
│ Director     │       ┌─────────────────┐
│ Plot         │       │    ratings      │
│ BoxOffice    │       ├─────────────────┤
│ imdbRating   │       │ userId          │
│ ...          │       │ movieId (FK)────┼───┐
└──────────────┘       │ rating          │   │
                       │ timestamp       │   │
                       └─────────────────┘   │
                                             │
                            (relationship)───┘
Tables
1. movies - Core movie information
Column	Type	Description
movieId	INTEGER	Primary key
title	TEXT	Original title with year
clean_title	TEXT	Title without year
year	INTEGER	Release year
decade	INTEGER	Decade (feature engineering)
genres	TEXT	Pipe-separated genre string
Director	TEXT	From OMDb API
Plot	TEXT	From OMDb API
BoxOffice	TEXT	From OMDb API
BoxOfficeParsed	INTEGER	Numeric box office value
Runtime	TEXT	From OMDb API
Country	TEXT	From OMDb API
imdbRating	TEXT	From OMDb API
2. genres - Normalized genre names
Column	Type	Description
genre_id	INTEGER	Primary key
genre_name	TEXT	Unique genre name
3. movie_genres - Many-to-many relationship
Column	Type	Description
movie_id	INTEGER	Foreign key to movies
genre_id	INTEGER	Foreign key to genres
4. ratings - User ratings
Column	Type	Description
userId	INTEGER	User identifier
movieId	INTEGER	Foreign key to movies
rating	REAL	Rating value (0-5)
timestamp	INTEGER	Unix timestamp
Design Principles
Normalization: Genres stored separately to avoid redundancy
Relationships: Proper foreign keys ensure data integrity
Indexes: Strategic indexes on frequently queried columns
Feature Engineering: Decade column enables temporal analysis
Dual Storage: Both raw (BoxOffice) and parsed (BoxOfficeParsed) values
📊 Analytical Queries
Run the analytical queries:

bash
sqlite3 movies.db < queries.sql
Or run interactively:

bash
sqlite3 movies.db
.read queries.sql
Required Queries
Query 1: Highest Average Rating
sql
-- Movie with highest average rating (min 10 ratings)
SELECT clean_title, year, avg_rating, num_ratings
FROM (...)
LIMIT 1;
Query 2: Top 5 Genres by Rating
sql
-- Top 5 genres with highest average rating
SELECT genre_name, avg_rating, num_movies, total_ratings
FROM (...)
LIMIT 5;
Query 3: Most Prolific Director
sql
-- Director with most movies
SELECT Director, movie_count, movie_titles
FROM (...)
LIMIT 1;
Query 4: Ratings by Year
sql
-- Average rating trends over time
SELECT release_year, avg_rating, num_movies
FROM (...)
ORDER BY release_year;
Bonus Queries Included
Top 10 most popular movies (by rating count)
Genre distribution across decades
Highest IMDB rated movies
Data quality and enrichment success rates
🎨 Design Decisions
1. SQLite Database Choice
Why: Zero configuration, portable, perfect for development
Production: Would migrate to PostgreSQL for concurrency and scalability
2. Normalized Genre Schema
Why: Avoids data redundancy and enables efficient genre-based queries
Trade-off: Slightly more complex joins, but cleaner data model
3. Idempotent Pipeline
Why: Safe to run multiple times; drops and recreates tables
Implementation: DROP TABLE IF EXISTS before creation
4. Rate Limiting
Why: Respects OMDb API limits (1,000 requests/day for free tier)
Implementation: Configurable delay between API calls
5. Graceful Error Handling
Why: API failures shouldn't stop entire pipeline
Implementation: Returns 'N/A' for failed API calls, logs warnings
6. Feature Engineering
Why: Decade column enables temporal analysis without complex calculations
Implementation: decade = (year // 10) * 10
7. Statistical Filtering
Why: Movies with few ratings can skew averages
Implementation: Minimum 10 ratings for top movie, 100 for top genres
🛠️ Challenges & Solutions
Challenge 1: API Title Matching
Problem: Movie titles in CSV may not exactly match OMDb database

Example: "Toy Story (1995)" vs "Toy Story"
Solution:

python
# Extract year and clean title
movies['year'] = movies['title'].apply(extract_year)
movies['clean_title'] = movies['title'].apply(clean_title)

# Use both in API call
fetch_omdb_details(clean_title, year)
Challenge 2: API Rate Limiting
Problem: Free tier limited to 1,000 requests/day

Solution:

Implemented configurable delay between calls
Added processing limit for testing
Comprehensive logging to track API usage
python
API_RATE_LIMIT_DELAY = 0.3  # seconds
time.sleep(API_RATE_LIMIT_DELAY)
Challenge 3: Genre Data Structure
Problem: Genres stored as pipe-separated string "Action|Thriller|Drama"

Solution:

Parse into list during transformation
Create normalized genres table
Build many-to-many relationship table
Challenge 4: Missing or Invalid Data
Problem: API doesn't return data for all movies

Solution:

Return 'N/A' for missing fields
Log warnings but continue processing
Filter 'N/A' values in analytical queries
Challenge 5: Idempotency
Problem: Running pipeline twice would duplicate data

Solution:

Drop existing tables before creation
Replace data instead of appending
Clear tables before each load
🔮 Production Improvements
Scalability Enhancements
Database Migration
python
   # Switch to PostgreSQL for production
   DATABASE = "postgresql://user:pass@host:5432/movies"
Parallel API Calls
python
   from concurrent.futures import ThreadPoolExecutor
   
   with ThreadPoolExecutor(max_workers=5) as executor:
       results = executor.map(fetch_omdb_details, movie_titles)
Caching Layer
python
   import redis
   cache = redis.Redis()
   
   # Cache API responses to avoid repeated calls
   cached_result = cache.get(movie_title)
Incremental Updates
python
   # Process only new movies since last run
   last_processed_date = get_last_run_date()
   new_movies = movies[movies['created_at'] > last_processed_date]
Airflow Integration
python
   # Schedule daily runs
   from airflow import DAG
   dag = DAG('movie_pipeline', schedule_interval='@daily')
Data Quality Tests
python
   # Add validation checks
   assert len(movies) > 0, "No movies loaded"
   assert movies['year'].notna().sum() > 0.8 * len(movies)
Monitoring & Alerts
python
   # Send alerts on failure
   if not success:
       send_slack_alert("Pipeline failed!")
🐛 Troubleshooting
Error: ModuleNotFoundError: No module named 'pandas'
Solution:

bash
pip install pandas requests sqlalchemy
Error: FileNotFoundError: [Errno 2] No such file or directory: 'movies.csv'
Solution:

Ensure movies.csv and ratings.csv are in the same folder as etl.py
Check file names are exactly: movies.csv and ratings.csv
Error: API returns "Invalid API key"
Solution:

Verify your API key is correct in etl.py line 24
Check you activated your key via the email confirmation
Free keys are limited to 1,000 requests/day
Error: database is locked
Solution:

bash
# Close all connections to database
# Delete movies.db and run again
rm movies.db
python etl.py
Warning: "No OMDb data found for: [movie title]"
Expected Behavior: Not all movies exist in OMDb database. Pipeline continues with 'N/A' values.

Pipeline runs too slowly
Solution:

python
# Reduce movies to process (line 26):
MAX_MOVIES_TO_PROCESS = 50

# Or increase delay if getting rate limited (line 28):
API_RATE_LIMIT_DELAY = 0.5
SQLite specific limitations
Known Issues:

GROUP_CONCAT is SQLite-specific (PostgreSQL uses STRING_AGG)
AUTOINCREMENT is SQLite-specific (PostgreSQL uses SERIAL)
Current schema works perfectly with SQLite
If using PostgreSQL: Modify the SQL syntax accordingly.

📝 Assumptions Made
Data Quality: MovieLens CSV files are well-formatted and complete
API Availability: OMDb API is accessible and responsive
Processing Limit: Default 100 movies is sufficient for demonstration
Network: Stable internet connection for API calls
Storage: Sufficient disk space for database (~50MB for full dataset)
Python Version: Python 3.7+ with standard libraries available
🎤 Presentation Tips
Key Points to Highlight (15-20 minutes)
1. Architecture (3 min)

Show the flow: CSV → Extract → Transform → API Enrich → Load → Query
Emphasize multi-source integration
2. Database Design (3 min)

Walk through normalized schema
Explain many-to-many relationship for genres
Highlight indexes for performance
3. Code Walkthrough (5 min)

Show main ETL functions: extract, transform, load
Point out error handling and logging
Demonstrate idempotency
4. Results (3 min)

Run queries live and show outputs
Discuss insights found in data
5. Challenges (3 min)

Title matching solution
Rate limiting handling
Missing data graceful handling
6. Production Readiness (3 min)

Discuss scalability improvements
Migration to PostgreSQL
Airflow scheduling
Monitoring and alerting
✅ Evaluation Checklist
Before presenting, verify:

 Pipeline runs successfully
 All 4 tables created in database
 Analytical queries return results
 Code is well-commented
 Logging works (check etl_pipeline.log)
 README is complete
 Error handling demonstrated
 Can explain design decisions
 Know scalability improvements
📞 Support
If you encounter issues:

## 📊 Running Analytical Queries

After the pipeline completes, you can run the analytical queries using two methods:

### Method 1: Python Script (Recommended - Best Formatting)

```bash
python run_queries.py
```

**Features:**
- ✅ Automatically runs all 4 required queries
- ✅ Beautiful pandas DataFrame formatting
- ✅ Includes bonus queries and data quality summary
- ✅ Shows API enrichment success rates
- ✅ Easy to read and present

**Example Output:**
```
================================================================================
QUERY 1: Which movie has the highest average rating?
================================================================================
            movie_title  year  avg_rating  num_ratings
 Shawshank Redemption, The  1994        4.45          317

================================================================================
QUERY 2: What are the top 5 movie genres with highest average rating?
================================================================================
  genre_name  avg_rating  num_movies  total_ratings
   Film-Noir        4.05           7            423
 Documentary        3.95          12            567
         War        3.89          15            892
...
```

### Method 2: Direct SQLite (Traditional)

```bash
# Run all queries from SQL file
sqlite3 movies.db < queries.sql

# Or run interactively
sqlite3 movies.db
.mode column
.headers on
.read queries.sql
.quit
```

### Method 3: Individual Queries

```bash
# Query 1: Highest rated movie
sqlite3 movies.db "SELECT m.clean_title, m.year, ROUND(AVG(r.rating), 2) AS avg_rating FROM movies m JOIN ratings r ON m.movieId = r.movieId GROUP BY m.movieId HAVING COUNT(r.rating) >= 10 ORDER BY avg_rating DESC LIMIT 1;"

# Query 2: Top genres
sqlite3 movies.db "SELECT g.genre_name, ROUND(AVG(r.rating), 2) AS avg_rating FROM genres g JOIN movie_genres mg ON g.genre_id = mg.genre_id JOIN movies m ON mg.movie_id = m.movieId JOIN ratings r ON m.movieId = r.movieId GROUP BY g.genre_name HAVING COUNT(r.rating) >= 100 ORDER BY avg_rating DESC LIMIT 5;"
```

---

## 🎯 Query Results

The pipeline answers these analytical questions:

1. **Which movie has the highest average rating?**
   - Uses minimum 10 ratings for statistical significance
   - Returns movie title, year, average rating, and rating count

2. **What are the top 5 movie genres with highest average rating?**
   - Uses normalized genre tables
   - Requires minimum 100 ratings per genre
   - Shows genre name, average rating, movie count, and total ratings

3. **Who is the director with the most movies in this dataset?**
   - Filters out 'N/A' entries (movies without API data)
   - Returns director name, movie count, and list of titles

4. **What is the average rating of movies released each year?**
   - Groups ratings by release year
   - Shows trends over time
   - Includes movie count and total ratings per year

### Bonus Queries Included

The `run_queries.py` script also provides:
- 🎬 Top 10 most popular movies (by rating count)
- 📊 Genre distribution across decades
- ⭐ Highest IMDB rated movies from API enrichment
- 📈 Data quality and API enrichment success rates

Check the etl_pipeline.log file for detailed error messages
Verify all prerequisites are installed
Confirm CSV files are in correct location
Validate API key is active
📄 License
This project is created for educational purposes as part of a technical assessment for tsworks.

🙏 Acknowledgments
MovieLens: For providing the movie dataset
OMDb API: For movie enrichment data
tsworks: For the opportunity to work on this challenge
Created by: [Your Name]
Date: November 2025
Contact: [Your Email]
GitHub: [Your GitHub Profile]

🚀 Quick Start Summary
bash
# 1. Download MovieLens data to project folder
# 2. Update API key in etl.py (line 24)
# 3. Install dependencies
pip install pandas requests sqlalchemy

# 4. Run the pipeline
python etl.py

# 5. View results
sqlite3 movies.db < queries.sql
That's it! Your pipeline is ready to present! 🎉

