package com.enriquesampaio.frauddetection

import com.enriquesampaio.frauddetection.machinelearning.KNN
import com.enriquesampaio.frauddetection.util.{Normalization, StratifiedSampling}
import org.apache.spark.{SparkConf, SparkContext}

object FraudDetection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Fraud Detection").setMaster("local[4]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("Choose an option: ")
    println("[1] Normalize")
    println("[2] Stratified Sampling")
    println("[3] K-NN")
    println("[0] Quit")

    var option = readLine()

    while (option != "0") {
      option match {
        case "1" => {
          new Normalization("resources/creditcard.csv", "output/creditcard_norm.csv").normalize(sc)
        }
        case "2" => {
          new StratifiedSampling("resources/creditcard.csv", "output/creditcard_train.csv", "output/creditcard_test.csv", 0.7).stratify(sc)
        }
        case "3" => {
          println("Insert K value: ")
          val k = readLine().toInt
          println("Accuracy: " + new KNN(k, "output/creditcard_train.csv", "output/creditcard_test.csv").train(sc))
        }
        case _ => println("Invalid option!")
      }

      println("Choose an option: ")
      println("[1] Normalize")
      println("[2] Stratified Sampling")
      println("[3] K-NN")
      println("[0] Quit")

      option = readLine()
    }

    sc.stop()
  }
}
