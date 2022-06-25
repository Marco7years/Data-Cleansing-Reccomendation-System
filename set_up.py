import mysql.connector
from neo4j import GraphDatabase
from random import randrange


#The number of users to create
N = 500001



'''
This function takes as arguments a query string and a
neo4j session. It runs the query.
'''
def execute_neo4j_query(q,session):
    result = session.run(q)
    return result

'''
This function takes as arguments an mysql connection and
a query string. It runs the query.
'''
def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    return 0

'''
This function takes as arguments an mysql connection and
a SELECT query string. It runs the query, returning a list
with the results.
'''
def execute_select(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    results = []
    for elem in cursor:
        results.append(elem)
    conn.commit()
    cursor.close()
    return results

'''
This functions is needed to open a csv file, returning
a matrix with as many rows as the ones contained into 
the csv and as many columns as the number of attributes
'''
def get_file(path,split_char):
    file = open(path, "r")
    lines = file.readlines()
    matrix = []
    for line in lines:
        row = line.replace('\n','').split(split_char)
        matrix.append(row)
    file.close()
    return matrix

'''
This function, having the files with general names
and surnames of people, it connects to a specific mysql
database (you have to create it and fill the CONNECTION
FUNCTION) and it fills it with random usesrs and random
'following' relations.
'''
def set_up_relational_database():
    conn = mysql.connector.connect(user='python', password='dmcourse2021',host='127.0.0.1',database='dm2021-2022',auth_plugin='mysql_native_password')
    names = get_file('csv/names.csv',',')
    surnames = get_file('csv/surnames.csv',',')
    queries = ["INSERT INTO users VALUES (1,'Alice','Smith',27);"]

    print("Generating random users")
    for i in range(2,N):
        names_index = randrange(0,len(names)-1)
        surnames_index = randrange(0,len(surnames)-1)

        chosen_name = str(names[names_index][0])
        chosen_surname = str(surnames[surnames_index][0])
        chosen_age = randrange(18,90)

        q = "INSERT INTO users VALUES ("+str(i)+",'"+chosen_name+"','"+chosen_surname+"',"+str(chosen_age)+")"
        queries.append(q)

    print("Generating random following of users")
    for i in range(1,N):
        number_of_friends = randrange(9,12)
        list_of_friends = []
        for j in range(0,number_of_friends):
            friend = randrange(1,N-1)
            while friend == i:
                friend = randrange(1,N-1)
            list_of_friends.append(friend)
        
        for f in list_of_friends:
            q = "INSERT INTO following VALUES ("+str(i)+","+str(f)+")"
            queries.append(q)
    
    i = 0
    print("Executing insertions of users and following")
    for q in queries:
        if i % 5000 == 0:
            print(str(i)+"/"+str(len(queries)))
        execute_query(conn,q)
        i+=1

    conn.close()

'''
This function, it issues two select queries to the mysql
database, in order to set up the corresponding graph database
(equivalent to the mysql one).
'''
def set_up_graph_database():
    conn = mysql.connector.connect(user='python', password='dmcourse2021',host='127.0.0.1',database='dm2021-2022')
    users_query = "SELECT * from users"
    friend_of_query = "SELECT * from following"
    print("Fetching users from relational dbms")
    users = execute_select(conn,users_query)
    print("Fetching friend_of from relational dbms")
    friend_of = execute_select(conn, friend_of_query)
    conn.close()

    queries = []
    # It issues the connection to the BOLT driver (neo4j)
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "dm2021"))
    session = driver.session()
    print("Connection established with graph dbms (neo4j).")

    print("Collecting queries to create users (nodes) -->"+str(len(users)))
    for u in users:
        id,name,surname,age = u
        q = "create (:User{uid: '"+str(id)+"', name: '"+str(name)+"', surname: '"+str(surname)+"', age: "+str(age)+"})"
        queries.append(q)

    print("Adding the query to create an index on user_id")
    #queries.append("CREATE INDEX ON :User(uid)")

    print("Collecting queries to create friendship relations (edges) -->"+str(len(friend_of)))
    for f in friend_of:
        id1, id2 = f 
        q = "match (u1:User), (u2:User) where u1.uid = '"+str(id1)+"' and u2.uid = '"+str(id2)+"' create (u1)-[:follows]->(u2)"
        queries.append(q)


    print("Running nodes and edges creation queries!")
    total_queries = len(queries)
    i = 0
    for q in queries:
        if i % 5000 == 0:
            print(str(i)+"/"+str(total_queries))
        execute_neo4j_query(q,session)
        i+=1

    print("Completed.")
    session.close()
    driver.close()


set_up_relational_database()
set_up_graph_database()