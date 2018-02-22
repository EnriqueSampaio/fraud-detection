package com.enriquesampaio.frauddetection

import com.enriquesampaio.frauddetection.machinelearning.KNN
import com.enriquesampaio.frauddetection.util.{Normalization, StratifiedSampling}
import org.apache.spark.{SparkConf, SparkContext}

object FraudDetection {
  def main(args: Array[String]): Unit = {
    val usage =
      """
        |Usage: spark-submit frad-detection.jar --master masterNode normalize|sample|knn [case sample -> (train percentage) double] [case knn -> (k) int]
      """.stripMargin
    if (args.length == 0) println(usage)

    val conf = new SparkConf().setAppName("Fraud Detection")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    args(0) match {
      case "normalize" => {
        new Normalization("resources/creditcard.csv", "output/creditcard_norm.csv").normalize(sc)
      }
      case "sample" => {
        if (args(1) !== null) {
          new StratifiedSampling("resources/creditcard.csv", "output/creditcard_train.csv", "output/creditcard_test.csv", args(1).toDouble).stratify(sc)
        }
        new StratifiedSampling("resources/creditcard.csv", "output/creditcard_train.csv", "output/creditcard_test.csv", 0.7).stratify(sc)
      }
      case "knn" => {
        if (args(1) !== null) {
          println(new KNN(args(1).toInt, "output/creditcard_train.csv", "output/creditcard_test.csv").train(sc))
        }
        println(new KNN(3, "output/creditcard_train.csv", "output/creditcard_test.csv").train(sc))
      }
      case _ => println("Invalid option!")
    }

    sc.stop()
  }
}
