import time
from neo4j import GraphDatabase
import pandas as pd

class Neo4jConnection:

    def __init__(self, uri: str, user: str, pwd: str, db: str = None):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        self.__db = db
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query: str, parameters=None) -> list:
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
 
        try: 
            session = self.__driver.session(database=self.__db) if self.__db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


def add_movies(movies: pd.DataFrame, batch_size: int = 3000):
    # Adds movie nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (c:Movie {movieId: row.movieId, title: row.title})
            RETURN count(*) as total
            '''

    return insert_data(query, movies, batch_size)


def add_genres(genres: pd.DataFrame, batch_size: int = 300):
    # Adds genres nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (c:Genre {name: row.name})
            RETURN count(*) as total
            '''
                        # $rows is substituted with the following dictionary
    return insert_data(query, genres, batch_size)

def add_has_genres(movies_with_genres: pd.DataFrame, batch_size: int = 5000):
    # Adds has_genres relationship to the Neo4j graph.
    query = ''' 
            UNWIND $rows AS row
            MERGE (m:Movie {title : row.title})
            
            WITH m, row
            UNWIND row.genres AS genre
            MATCH (g:Genre {name : genre})
            MERGE (m)-[:HAS_GENRE]->(g)
            RETURN count(*) as total
            '''
    return insert_data(query, movies_with_genres, batch_size)

def add_users(users: pd.DataFrame, batch_size: int = 300):
    # Adds user nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (u:User {userId: row.userId, name: row.name})
            RETURN count(*) as total
            '''

    return insert_data(query, users, batch_size)

def add_rel_rated(ratings: pd.DataFrame, batch_size: int = 10000):
    # Adds user nodes to the Neo4j graph.
    query = '''
            UNWIND $rows as row
            MATCH (u:User {userId: row.userId}), (m:Movie {movieId : row.movieId})
            MERGE (u)-[r:RATED {rating : row.rating, timestamp : row.timestamp}]->(m)
            RETURN count(*) as total
            '''

    return insert_data(query, ratings, batch_size)

def insert_data(query: str, rows: pd.DataFrame, batch_size: int):
    # Function to handle the updating the Neo4j database in batch mode.
    total = 0
    batch = 0
    start = time.time()
    result = None
    
    while batch * batch_size < len(rows):
        
        # Considers a batch of rows at a time
        res = conn.query(query, 
                         parameters = {'rows': rows[batch*batch_size : (batch+1)*batch_size].to_dict('records')})
        """
        A record is a dictionary with this structure:
            {attribute_1: value, attribute_2: value, ecc.}
            {'movieId': 1, 'title': 'Toy Story'}, 
            {'movieId': 2, 'title': 'Jumanji'}, 
            {'movieId': 3, 'title': 'Grumpier Old Men'}
        """
        
        total += res[0]['total']
        batch += 1
        result = {"total": total, 
                  "batches": batch, 
                  "time": time.time()-start}
        print(result)
        
    return result


if __name__ == "__main__":
    conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "ciao", "recommendation-movies")

    # Retrieve Movies
    path_movies = "./datasets/cleaned_movies.csv"
    df_movies = pd.read_csv(path_movies)

    df_movies['genres'] = df_movies['genres'].apply(lambda x: x.split("|"))

    # Create nodes for movies
    # add_movies(df_movies[['movieId', 'title', 'year']], batch_size=3000)

    # Retrieve Genres
    path_genres = "./datasets/genres.csv"
    df_genres = pd.read_csv(path_genres)

    # Create nodes for genres
    # add_genres(df_genres)

    # Create relationship between Movie and Genre
    # add_has_genres(df_movies[['title', 'genres']], batch_size=5000)

    # Retrieve Users
    path_users = './datasets/users.csv'
    df_users = pd.read_csv(path_users)

    # Create nodes for users
    # add_users(df_users)

    # Retrieve Ratings
    path_ratings = './datasets/ratings.csv'
    df_ratings = pd.read_csv(path_ratings)

    # Create users and relationships with movies through ratings
    add_rel_rated(df_ratings, 10000)

    conn.close()