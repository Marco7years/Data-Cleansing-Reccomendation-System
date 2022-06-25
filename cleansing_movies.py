import pandas as pd

# Ratings
path_ratings = "./datasets/ratings.csv"
df_ratings = pd.read_csv(path_ratings)

# TODO: associate names with ids


# Movies
path_movies = "./datasets/movies.csv"
df_movies = pd.read_csv(path_movies)

# Check if all movies have the year in the title
df_movies['title'] = df_movies['title'].apply(lambda x: x.rstrip())
no_year = df_movies['title'].apply(lambda x : not x[-5:-1].isdigit())

# Movies without year
movies_no_year = df_movies['title'][no_year]

# Retrieve years
df_movies['year'] = df_movies['title'].apply(lambda x : x[-5:-1] if x[-5:-1].isdigit() else None)

# Remove year from 'title'
df_movies['title'] = df_movies['title'][~no_year].apply(lambda x: x[:-6].rstrip())

# print(df_movies.sample(50))

path_movies_clean = "./datasets/cleaned_movies.csv"
# Create cleaned dataset
# df_movies.to_csv(path_movies_clean, index=False)