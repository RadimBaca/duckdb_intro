import duckdb

con = duckdb.connect(database=':memory:', read_only=False)

movie_data = con.sql(r"""
    SELECT startYear, g, COUNT(*) AS count
    FROM read_csv_auto('hf://datasets/ArchaeonSeq/MovieImdb/df.csv'),    
         UNNEST(STRING_SPLIT(TRIM(BOTH '[]' FROM REPLACE(genres, '''', '')), ', ')) AS genre(g)
    WHERE g = 'Horror'
    GROUP BY startYear, g
    ORDER BY startYear DESC
    """)

movie_data.to_csv('movie_data.csv')
movie_data.to_parquet('movie_data.parquet')
print(movie_data)

con.close()