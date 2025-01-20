import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GitHubCommunityDetection {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GitHubCommunityDetection")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val edgesFile = "path/to/github-social-network.csv"
    val edges: RDD[Edge[Double]] = spark.read
      .option("header", "false")
      .csv(edgesFile)
      .rdd
      .map { row => 
        Edge(
          row.getString(0).toLong, 
          row.getString(1).toLong, 
          1.0
        )
      }

    val graph = Graph.fromEdges(edges, defaultValue = 0.0)

    def affiliationGraphModel(graph: Graph[Double, Double], iterationsMax: Int): Graph[VertexId, Double] = {
      var communities = graph.mapVertices((vid, _) => vid)
      
      for (_ <- 1 to iterationsMax) {
        val communityAffiliations = communities.aggregateMessages[Set[VertexId]](
          triplet => {
            val srcCommunity = triplet.srcAttr
            val dstCommunity = triplet.dstAttr
            if (srcCommunity != dstCommunity) {
              triplet.sendToBoth(Set(srcCommunity, dstCommunity))
            }
          },
          (a, b) => a ++ b
        )
        
        communities = communities.outerJoinVertices(communityAffiliations) {
          (vid, oldCommunity, affiliationOpt) => 
            affiliationOpt.getOrElse(Set(oldCommunity)).minBy(identity)
        }
      }

      communities
    }

    def computeModularity(graph: Graph[VertexId, Double]): Double = {
      val communities = graph.vertices.map(_._2).distinct().collect()
      val totalDegree = graph.degrees.map(_._2.toDouble).sum()
      
      communities.map { community =>
        val communityVertices = graph.vertices.filter(_._2 == community).map(_._1).collect()
        val internalEdges = graph.triplets
          .filter(triplet => 
            communityVertices.contains(triplet.srcId) && 
            communityVertices.contains(triplet.dstId)
          ).count()
        
        val communityDegree = graph.degrees
          .filter(vd => communityVertices.contains(vd._1))
          .map(_._2.toDouble)
          .sum()
        
        val expectedEdges = (communityDegree / totalDegree) * (communityDegree / totalDegree)
        (internalEdges / totalDegree) - expectedEdges
      }.sum
    }

    val communitiesGraph = affiliationGraphModel(graph, iterationsMax = 5)
  
    communitiesGraph.vertices.groupBy(_._2)
      .mapValues(_.size)
      .foreach { case (communityId, size) =>
        println(s"Community $communityId: $size vertices")
      }

    val modularity = computeModularity(communitiesGraph)
    println(s"Modularity: $modularity")

    sc.stop()
  }
}