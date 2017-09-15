package frauddetection

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KNN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Fraud Detection").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val teste = Array(170348,1.9919760961759,0.158475887304227,-2.58344064503516,0.408669992998441,1.15114706077937,-0.0966947441848027,0.223050267455537,-0.0683838777747007,0.577829383844873,-0.888721675865145,0.491140241656789,0.728903319843614,0.380428045513993,-1.94888334870021,-0.832498136300872,0.519435549203291,0.903562376617253,1.19731471799372,0.593508846946918,-0.0176522567052908,-0.164350327825504,-0.295135166851559,-0.0721725311018398,-0.450261313423321,0.313266608995469,-0.289616585696882,0.00298758224342907,-0.0153088128485981,42.53)

    val neighbours = sc.textFile("resources/creditcard.csv")
      .map(line => line.split(","))
      .map(line => (line(30), line.slice(0, 29)))
      .map { case (target, feature) =>
        (target, scala.math.sqrt(feature.map(feature => feature.toDouble).zip(teste).map { case (x, y) => scala.math.pow(x - y, 2) }.sum)) }
      .takeOrdered(3)(Ordering[Double].on(pair => pair._2))
      .map{ case (target, dist) => (target, 1) }

    val result = sc.parallelize(neighbours).reduceByKey(_ + _)
      .reduce((x, y) => if (x._2 > y._2) x else y)._1
    println(result)

    sc.stop()
  }
}
