package com.org.inf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable.Set

object DeviceCom {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("usage: spark-submit com.org.inf.DeviceCom <input> <output> <iternum>")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.setAppName("DeviceCom-" + System.getenv("USER"))

    val sc = new SparkContext(conf)

    val input = args(0)

    val output = args(1)

    val iternum = args(2).toInt

    val vids = sc.textFile(input)
      .flatMap(line => line.split("\t").take(2))
      .distinct
      .zipWithUniqueId()
      .map(x => (x._1, x._2.toLong))

    val vids_map = sc.broadcast(vids.collectAsMap())

    val vids_rdd = vids.map {
      case (username, userid) =>
        (userid, username)
    }

    val raw_edge = sc.textFile(input)
      .map(line => line.split("\t"))
    val col = raw_edge.collect()

    val edges_rdd = sc.parallelize(col.map {
      case (x) =>
        (vids_map.value(x(0)), vids_map.value(x(1)))
    })

    val g = Graph.fromEdgeTuples(edges_rdd, 1)
    val lp = lib.LabelPropagation.run(g, iternum).vertices
    val LpByUsername = vids_rdd.join(lp).map {
      case (id, (username, label)) =>
        (username, label)
    }

    LpByUsername.map(x => x._1 + "\t" + x._2).saveAsTextFile(output)

    sc.stop()
  }
}
