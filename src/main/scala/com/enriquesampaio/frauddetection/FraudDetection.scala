package com.enriquesampaio.frauddetection

import com.enriquesampaio.frauddetection.machinelearning.KNN
import com.enriquesampaio.frauddetection.util._
import org.apache.spark.{SparkConf, SparkContext}

object FraudDetection {
  def main(args: Array[String]): Unit = {
    val usage =
      """
        |Usage: spark-submit frad-detection.jar normalize|sample|knn [case sample -> (train percentage) double] [case knn -> (k) int]
      """.stripMargin
    if (args.length < 1) {
      println(usage)
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Fraud Detection").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    args(0) match {
      case "scale" => {
        saveAsRdd("resources/creditcard.csv", sc)
        scale(sc)
      }
      case "sample" => {
        if (args.length == 2) {
          stratify(args(1).toDouble, sc)
        }
        stratify(0.7, sc)
      }
      case "knn" => {
        if (args.length == 2) {
          new KNN(args(1).toInt).train(sc)
        }
        new KNN(3).train(sc)
      }
      case _ => println("Invalid Option!")
    }

    sc.stop()
  }
}
