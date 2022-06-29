# Installation Guide

Assuming having python3 installed on your machine, you have to installed the following libraries:

> pip3 install neo4j

> pip3 install pandas

Then you have to install [neo4j desktop](https://neo4j.com/) or it is possible to use the [sandbox version of neo4j](https://neo4j.com/sandbox/) to run the system remotely.

Create a Neo4j DBMS named 'Recommendation-System' with password 'dmproject'.

**Note**: It is possible to create a new database different from the default one.
      In such case edit the variable 'db' in 'create_graph_database.py' to assign the name of the new db created.

Once the DBMS is created, you have to install the following plugins from the 'plugins' section that should appear on the right:

> APOC

> Graph Data Science Library


The graph database is created by running the 'create_graph_database.py' script.

Finally, you can execute the queries in the 'query_in_neo4j.txt' file on the Neo4j system and build the recommendation system.

