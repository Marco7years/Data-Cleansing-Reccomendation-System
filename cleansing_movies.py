import pandas as pd

# Movies
path_movies = "./datasets/movies.csv"
df_movies = pd.read_csv(path_movies)

# Check if all movies have the year in the title
df_movies['title'] = df_movies['title'].apply(lambda x: x.rstrip())
no_year = df_movies['title'].apply(lambda x : not x[-5:-1].isdigit())

# any returns True if there are movies that do not have the year
print(no_year.any()) # Output: True -> There exists at least one movie without year

# df_movies[no_year].to_csv("./datasets/no_year.csv", index=False) 
print(df_movies[no_year])

# Movies without year
movies_no_year = df_movies['title'][no_year]

# Retrieve years (The year is contained in the last 6 character of the title)
df_movies['year'] = df_movies['title'].apply(lambda x : x[-5:-1] if x[-5:-1].isdigit() else None)

# Remove year from 'title'
df_movies['title'] = df_movies['title'].apply(lambda x: x[:-6].rstrip() if x[-5:-1].isdigit() else x)

# Create cleaned dataset
path_movies_clean = "./datasets/cleaned_movies.csv"
# df_movies.to_csv(path_movies_clean, index=False)