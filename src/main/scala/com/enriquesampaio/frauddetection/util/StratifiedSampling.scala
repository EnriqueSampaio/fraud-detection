package com.enriquesampaio.frauddetection.util

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object StratifiedSampling {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Fraud Detection").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rows = sc.textFile("resources/creditcard.csv")
      .map(row => row.split(","))
      .map(row => (row(30)(1), row.slice(0,30)))

    val count = rows.count()
    val targetsPercentage = rows.map(row => (row._1, 1)).reduceByKey(_ + _).map(target => (target._1, target._2.toDouble / count)).sortBy(_._1).collect()

    val trainSize = (count * 0.7).floor.toInt
    val testSize = count - trainSize

    val trainNegativeSize = (trainSize * targetsPercentage(0)._2).floor.toInt
    val trainPositiveSize = trainSize - trainNegativeSize

    val testNegativeSize = (testSize * targetsPercentage(0)._2).floor.toInt
    val testPositiveSize = testSize - testNegativeSize

    val negatives = rows.filter(row => row._1.equals('0')).randomSplit(Array(trainNegativeSize, testNegativeSize))
    val positives = rows.filter(row => row._1.equals('1')).randomSplit(Array(trainPositiveSize, testPositiveSize))

    val train = negatives(0).union(positives(0)).collect()
    val test = negatives(1).union(positives(1)).collect()

    val pwTest = new PrintWriter(new File("output/creditcard_test.csv"))

    test.map(row => row._2.mkString(",") + "," + row._1).foreach(row => pwTest.println(row))

    pwTest.close()

    val pwTrain = new PrintWriter(new File("output/creditcard_train.csv"))

    train.map(row => row._2.mkString(",") + "," + row._1).foreach(row => pwTrain.println(row))

    pwTrain.close()

    sc.stop()
  }
}
