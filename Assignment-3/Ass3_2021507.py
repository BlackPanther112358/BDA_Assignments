import os
import json
import numpy as np
from neo4j import GraphDatabase
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
from pyspark.sql import SparkSession
from tqdm import tqdm

url = os.getenv.get("DB_URL")
username = os.getenv.get("DB_UNAME")
password = os.getenv.get("DB_PASS")


def write_to_database(data):
    def batch_query(tx, batch):
        tx.run(
            """
            UNWIND $batch as row
            MERGE (p:Paper {id: row.paper})
            FOREACH (ref in row.reference |
                MERGE (r:Paper {id: ref})
                MERGE (p)-[:CITED]->(r)
            )
        """,
            batch=batch,
        )

    with GraphDatabase.driver(url, auth=(username, password)) as driver:
        with driver.session() as session:
            session.run("CREATE CONSTRAINT FOR (p:Paper) REQUIRE p.id IS UNIQUE")

            batch = []  
            for d in tqdm(data, desc="Writing to neo4j"):
                paper = d.get("paper")
                references = d.get("reference", [])

                batch.append({"paper": paper, "reference": references})

                if len(batch) == 10000:
                    session.execute_write(batch_query, batch)
                    batch = []
            
            if batch:
                session.execute_write(batch_query, batch)

def simrank_algo(driver, importance_factor=0.9, max_iterations=1000, tolerance=1e-4):
    spark = SparkSession.builder\
        .appName("SimRank")\
        .config("spark.sql.shuffle.partitions", 200)\
        .config("spark.default.parallelism", 200)\
        .config("spark.memory.fraction", 0.8)\
        .config("spark.executor.memory", "8g")\
        .config("spark.driver.memory", "10g")\
        .config("spark.memory.offHeap.enabled", True)\
        .config("spark.memory.offHeap.size", "8g")\
        .config("spark.memory.storageFraction", 0.3)\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    with driver.session() as session:

        result = session.run("""
        MATCH (n1:Paper)-[:CITED]->(n2:Paper)
        RETURN n1.id as node1, collect(n2.id) as neighbors
        """)

        neighbour_df = spark.createDataFrame(result, ["node1", "neighbors"])
        neighbour_df = neighbour_df.withColumnRenamed("node1", "node").cache()  # Cache the DataFrame

    node_list = neighbour_df.select("node").distinct().rdd.flatMap(lambda x: x).collect()
    tmp = neighbour_df.select("neighbors").distinct().rdd.flatMap(lambda x: x).collect()
    tmp = sc.parallelize(tmp).flatMap(lambda x: x).collect()
    node_list.extend(tmp)

    node_to_index = sc.broadcast({node: i for i, node in enumerate(node_list)})

    def create_adj_matrix(row):
        node = row.node
        neighbors = row.neighbors
        index = node_to_index.value[node]
        return [(index, node_to_index.value[neighbor], 1) for neighbor in neighbors]

    adj_matrix_entries = neighbour_df.rdd.flatMap(create_adj_matrix).cache()
    adj_matrix = CoordinateMatrix(adj_matrix_entries)

    neighbour_df.unpersist()

    column_sums = adj_matrix_entries\
        .map(lambda x: (x[1], x[2]))\
        .reduceByKey(lambda a, b: a + b)\
        .mapValues(lambda x: 1.0 if x == 0 else x)\
        .cache()

    normalized_entries = adj_matrix_entries\
        .map(lambda x: (x[1], (x[0], x[2])))\
        .join(column_sums)\
        .map(lambda x: MatrixEntry(x[1][0][0], x[0], x[1][0][1] / x[1][1]))\
        .cache()

    n = len(node_list)
    adj_matrix = CoordinateMatrix(normalized_entries, n, n).toBlockMatrix().cache()
    adj_matrix_t = adj_matrix.transpose().cache()

    column_sums.unpersist()
    adj_matrix_entries.unpersist()
    normalized_entries.unpersist()

    # create RDD of adj_matrix, adj_matrix_t

    identity_entries = sc.parallelize([(i, i, 1.0) for i in range(n)])
    identity_matrix = CoordinateMatrix(identity_entries, n, n).toBlockMatrix().cache()

    curr_score = identity_matrix
    importance_factor_b = sc.broadcast(importance_factor)

    # SimRank iterations
    for _ in tqdm(range(max_iterations)):
        prev_score = curr_score

        curr_score = adj_matrix_t.multiply(curr_score).multiply(adj_matrix)

        curr_score = curr_score.toCoordinateMatrix().entries\
            .map(lambda e: MatrixEntry(e.i, e.j, e.value * importance_factor_b.value))\
            .cache()

        curr_score = CoordinateMatrix(
            curr_score.filter(lambda e: e.i != e.j),
            n, n
        ).toBlockMatrix().add(identity_matrix).cache()

        diff = prev_score.subtract(curr_score).toCoordinateMatrix()
        avg_diff = diff.entries.map(lambda e: abs(e.value)).max()
        if avg_diff < tolerance:
            break
        prev_score.toCoordinateMatrix().entries.unpersist()

    return curr_score.toLocalMatrix().toArray()


def get_closest_nodes(simrank_matrix, query_index, top_k=5):
    
    if query_index < 0 or query_index >= simrank_matrix.shape[0]:
        raise ValueError(f"Query index '{query_index}' is out of bounds.")

    similarity_scores = simrank_matrix[query_index]
    similarity_scores[query_index] = -1
    top_indices = np.argsort(similarity_scores)[-top_k:][::-1]
    closest_nodes = [(index, similarity_scores[index]) for index in top_indices]

    return closest_nodes


if __name__ == "__main__":

    with open("train.json") as f:
        data = [json.loads(line) for line in f.readlines()]

    db_driver = GraphDatabase.driver(url, auth=(username, password))

    write_to_database(data)
    simrank_matrix = simrank_algo(db_driver)

    query_node = 2982615777
    closest_nodes = get_closest_nodes(simrank_matrix, query_node)
    print(f"Closest nodes to {query_node}: {closest_nodes}")

    db_driver.close()