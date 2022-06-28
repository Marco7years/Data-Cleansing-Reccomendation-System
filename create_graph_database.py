import time
from neo4j import GraphDatabase
import pandas as pd


# Class for managing the Neo4j Connection
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


# Subclass of Neo4jConnect that builds the graph database through queries
class RecommendationSystem(Neo4jConnection):
    def __init__(self, uri: str, user: str, pwd: str, db: str = None):
        self._conn = Neo4jConnection(uri, user, pwd, db)

    def close(self):
        self._conn.close()

    def read_input_data_and_create_db(self, path_movies: str, path_genres: str, 
                                            path_users: str, path_ratings: str):
        # Retrieve Movies
        df_movies = pd.read_csv(path_movies)
        df_movies['genres'] = df_movies['genres'].apply(lambda x: x.split("|"))

        # Create nodes for movies
        self.add_movies(df_movies[['movieId', 'title', 'year']])

        # Retrieve Genres
        df_genres = pd.read_csv(path_genres)

        # Create nodes for genres
        self.add_genres(df_genres)

        # Create HAS_GENRE relationship between Movie and Genre
        self.add_rel_has_genres(df_movies[['title', 'genres']])

        # Retrieve Users
        df_users = pd.read_csv(path_users)

        # Create nodes for users
        self.add_users(df_users)

        # Retrieve Ratings
        df_ratings = pd.read_csv(path_ratings)

        # Create RATED relationship between Movie and User
        self.add_rel_rated(df_ratings)



    def add_movies(self, movies: pd.DataFrame, batch_size: int = 3000):
        print('\nCreating Movies...')
        # Adds movie nodes to the Neo4j graph
        query = '''
                UNWIND $rows AS row
                MERGE (m:Movie {movieId: row.movieId, title: row.title})
                RETURN count(*) as total
                '''
        self._insert_data(query, movies, batch_size)
        print('Movies created')

        create_constraint = "CREATE CONSTRAINT FOR (m:Movie) REQUIRE m.movieId IS UNIQUE"
        res = self._conn.query(create_constraint)
        
        if res is not None:
            print("Unique constraint successfully created")

        return 


    def add_genres(self, genres: pd.DataFrame, batch_size: int = 300):
        print('\nCreating Genres...')
        # Adds genres nodes to the Neo4j graph
        query = '''
                UNWIND $rows AS row
                MERGE (c:Genre {name: row.name})
                RETURN count(*) as total
                '''
        return self._insert_data(query, genres, batch_size)

    def add_rel_has_genres(self, movies_with_genres: pd.DataFrame, batch_size: int = 5000):
        print('\nCreating relationships Movie-HAS_GENRE-Genre...')
        # Adds has_genres relationship to the Neo4j graph
        query = ''' 
                UNWIND $rows AS row
                MATCH (m:Movie {title : row.title})
                
                WITH m, row
                UNWIND row.genres AS genre
                MATCH (g:Genre {name : genre})
                MERGE (m)-[:HAS_GENRE]->(g)
                RETURN count(*) as total
                '''
        return self._insert_data(query, movies_with_genres, batch_size)

    def add_users(self, users: pd.DataFrame, batch_size: int = 300):
        print('\nCreating Users...')
        # Adds user nodes to the Neo4j graph
        query = '''
                UNWIND $rows AS row
                MERGE (u:User {userId: row.userId, name: row.name})
                RETURN count(*) as total
                '''
        self._insert_data(query, users, batch_size)
        print('Users created')

        create_constraint = "CREATE CONSTRAINT FOR (u:User) REQUIRE u.userId IS UNIQUE"
        res = self._conn.query(create_constraint)
        
        if res is not None:
            print("Unique constraint successfully created")
        
        return

    def add_rel_rated(self, ratings: pd.DataFrame, batch_size: int = 10000):
        print('\nCreating relationships User-RATED-Movie...')
        # Adds rated relationship to the Neo4j graph
        query = '''
                UNWIND $rows as row
                MATCH (u:User {userId: row.userId}), (m:Movie {movieId : row.movieId})
                MERGE (u)-[r:RATED {rating : row.rating, timestamp : row.timestamp}]->(m)
                RETURN count(*) as total
                '''

        return self._insert_data(query, ratings, batch_size)

    def ratings_per_movie(self, movieId : int):
        print('\nRetrieving the number of ratings per movie...')
        # Query to retrieve the number of ratings per movie
        query = '''
                MATCH (m:Movie {movieId: $movieId})-[r:RATED]-(u:User)
                RETURN m, count(u) AS num_ratings
                '''
        
        res = self._conn.query(query, parameters={'movieId': movieId})  
        
        # Returned parameters in the above query 
        params = ['m', 'num_ratings']
        self._parse_result(res, params)     
        
        return

    # Input type: 'neo4j.graph.Node'
    def _parse_result(self, res, params: list):
        for record in res:
            for idx, column in enumerate(record):
                print(params[idx], ": ", column)
        

    def _insert_data(self, query: str, rows: pd.DataFrame, batch_size: int):
        # Function to handle the insertion in batch mode
        total = 0
        batch = 0
        start = time.time()
        result = None
        
        while batch * batch_size < len(rows):
            
            # Considers a batch of rows at a time
            res = self._conn.query(query, 
                                parameters = {'rows': rows[batch*batch_size : (batch+1)*batch_size].to_dict('records')})
            """
            A record is a dictionary with this structure:
                {attribute_1: value, attribute_2: value, ecc.}
                {'movieId': 1, 'title': 'Toy Story', 'year': 1995}, 
                {'movieId': 2, 'title': 'Jumanji', 'year': 2001}, 
                {'movieId': 3, 'title': 'Grumpier Old Men', 'year': 2002}
            """
            
            total += res[0]['total'] # Retrive number of inserted records
            batch += 1
            result = {"total": total, 
                      "batches": batch, 
                      "time": time.time()-start}
            print(result)
            
        return result


if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    psw = "ciao"
    db = None

    rec_sys = RecommendationSystem(uri, user, psw, db)

    path_movies = "./datasets/cleaned_movies.csv"
    path_genres = "./datasets/genres.csv"
    path_users = './datasets/users.csv'
    path_ratings = './datasets/ratings.csv'


    # rec_sys.read_input_data_and_create_db(path_movies, path_genres, path_users, path_ratings)
    # Compute the number of rating for the given movie
    rec_sys.ratings_per_movie(5)

    rec_sys.close()