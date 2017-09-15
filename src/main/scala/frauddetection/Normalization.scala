package frauddetection

import java.io.PrintWriter
import java.io.File

import org.apache.spark.{SparkConf, SparkContext}


object Normalization {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Fraud Detection").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rows = sc.textFile("resources/creditcard.csv")
      .map(row => row.split(",").slice(0,29).map(value => value.toDouble))
    val count = rows.count()

    val means = rows.reduce{ case (x, y) =>
      x.zip(y).map { case (x, y) => x + y }
    }.map(featureSum => featureSum / count)

    val stddevs = rows.map(row =>
      row.zip(means).map{ case (feature, mean) => scala.math.pow(feature - mean, 2) }
    ).reduce{ case (x, y) =>
      x.zip(y).map { case (x, y) => x + y }
    }.map(dev => scala.math.sqrt(dev / count))

    val rowsNorm = rows.map(row =>
      row.zip(means).map{ case(feature, mean) => feature - mean }.zip(stddevs).map{ case(stage, stddev) => stage / stddev }
    ).collect()

    sc.stop()

    val pw = new PrintWriter(new File("output/creditcard_norm.csv"))

    rowsNorm.map(row => row.mkString(",")).foreach(row => pw.println(row))
    pw.close()
  }
}
