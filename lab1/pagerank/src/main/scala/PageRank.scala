import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

object PageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "data/graphx/Wiki-Vote.txt")
    var ranks = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      .mapVertices{ (id, attr) => 1.0 }
    val resetProb = 0.15
    for (_ <- 1 to 100) {
      ranks.cache()
      val updates = ranks.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
      ranks = ranks.outerJoinVertices(updates) {(id, oldRank, msgSumOpt) => resetProb + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)}
    }
    println(ranks.vertices.collect().sortBy(- _._2).slice(0,20).mkString("\n"))
    spark.stop()
  }
}
