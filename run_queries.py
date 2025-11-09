import sqlite3

def run_queries(db_path, queries_path):
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        with open(queries_path, "r") as f:
            sql = f.read()
            for query in sql.strip().split(";"):
                query = query.strip()
                if query:
                    print(f"Running query:\n{query}\n")
                    try:
                        c.execute(query)
                        result = c.fetchall()
                        for row in result:
                            print(row)
                    except Exception as e:
                        print(f"Error: {e}")
                    print('-' * 40)

if __name__ == "__main__":
    run_queries("movies.db", "queries.sql")