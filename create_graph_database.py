import time
from neo4j import GraphDatabase
import pandas as pd

class Neo4jConnection:

    def __init__(self, uri, user, pwd, db):
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

    def query(self, query, parameters=None):
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


def add_movies(movies, batch_size=50):
    # Adds category nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (c:Movie {movieId: row.movieId, title: row.title})
            RETURN count(*) as total
            '''

    return insert_data(query, movies, batch_size)

def insert_data(query, rows, batch_size = 10000):
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
        result = {"total":total, 
                  "batches":batch, 
                  "time":time.time()-start}
        print(result)
        
    return result


if __name__ == "__main__":
    conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "ciao", "recommendation-movies")
    # q = conn.query("CREATE (n:Person {name: 'Andy', title: 'Developer'})")

    # Retrieve Movies
    path_movies = "./datasets/cleaned_movies.csv"
    df_movies = pd.read_csv(path_movies)

    first_movies = df_movies[:100][['movieId', 'title']]
    add_movies(first_movies)

    conn.close()