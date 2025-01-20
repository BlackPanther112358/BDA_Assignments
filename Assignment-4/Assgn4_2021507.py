from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import ArrayType, FloatType
import numpy as np
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("AGM_GraphFrames") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

edges_path = "./git_web_ml/github-social.csv"
edges_df = spark.read.csv(edges_path, header=True, inferSchema=True)

vertices = edges_df.select(col("src").alias("id")) \
    .union(edges_df.select(col("dst").alias("id"))) \
    .distinct()

edges = edges_df.select(col("src").alias("src"), col("dst").alias("dst"))

num_communities = 5
def random_membership():
    return list(np.random.rand(num_communities))

random_membership_udf = udf(random_membership, ArrayType(FloatType()))
vertices = vertices.withColumn("membership", random_membership_udf())

g = GraphFrame(vertices, edges)

def normalize_membership(arr):
    total = sum(arr)
    return [x / total for x in arr] if total > 0 else arr

normalize_udf = udf(normalize_membership, ArrayType(FloatType()))

def update_membership(graph, num_communities):
    aggregated = graph.aggregateMessages(
        sendToSrc="dst.membership",  
        sendToDst="src.membership"   
    )
    
    updated_vertices = aggregated.withColumn(
        "membership", normalize_udf(col("aggregated"))
    )
    return GraphFrame(updated_vertices, graph.edges)

max_iterations = 10
for _ in range(max_iterations):
    g = update_membership(g, num_communities)

result = g.labelPropagation(maxIter=5)
communities = result.select("id", "label")

modularity = g.edges.join(
    communities.withColumnRenamed("id", "src").alias("src_comm"),
    on="src"
).join(
    communities.withColumnRenamed("id", "dst").alias("dst_comm"),
    on="dst"
).withColumn(
    "intra_comm", when(col("src_comm.label") == col("dst_comm.label"), 1).otherwise(0)
).select("intra_comm").groupBy().sum("intra_comm").collect()[0][0]

print(f"Modularity score: {modularity}")

communities.write.csv("./git_web_ml/communities", header=True)

spark.stop()
