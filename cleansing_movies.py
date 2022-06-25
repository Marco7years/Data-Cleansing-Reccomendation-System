import pandas as pd
import numpy as np

# # Ratings
# path_ratings = "./datasets/ratings.csv"
# df_ratings = pd.read_csv(path_ratings)

# # Names
# path_names = "./datasets/names.csv"
# df_names = pd.read_csv(path_names)

# num_users = len(df_ratings['userId'].unique())
# idx = pd.Index([i+1 for i in range(num_users)])

# path_ratings_names = "./datasets/ratings_names.csv"
# df_names['userId'] = idx

# # Join of names on userId
# # pd.merge(df_ratings, df_names, on="userId").to_csv(path_ratings_names, index=False)

# print(df_)

# Movies
path_movies = "./datasets/movies.csv"
df_movies = pd.read_csv(path_movies)

# Check if all movies have the year in the title
df_movies['title'] = df_movies['title'].apply(lambda x: x.rstrip())
no_year = df_movies['title'].apply(lambda x : not x[-5:-1].isdigit())
# no_year is not empty -> there are movies without year

# Movies without year
movies_no_year = df_movies['title'][no_year]

# Retrieve years
df_movies['year'] = df_movies['title'].apply(lambda x : x[-5:-1] if x[-5:-1].isdigit() else None)

# Remove year from 'title'
df_movies['title'] = df_movies['title'].apply(lambda x: x[:-6].rstrip() if x[-5:-1].isdigit() else x)

# print(df_movies.sample(50))

path_movies_clean = "./datasets/cleaned_movies.csv"
# Create cleaned dataset
# df_movies.to_csv(path_movies_clean, index=False)

