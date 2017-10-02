package com.enriquesampaio.frauddetection.machinelearning

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class KNN(private val k: Int, private val trainPath: String, private val testPath: String) {
  def train(): Unit = {
    val conf = new SparkConf().setAppName("Fraud Detection").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val testSample = sc.textFile(testPath).map(row => (row(30), row.split(",").slice(0,30).map(feature => feature.toDouble)))
    val trainSample = sc.textFile(trainPath).map(row => (row(30), row.split(",").slice(0,30).map(feature => feature.toDouble)))

    testSample.map(testRow => (testRow._1,
        trainSample.map(trainRow => (trainRow._1, scala.math.sqrt(distance(trainRow._2, testRow._2).sum)))
        .takeOrdered(k)(Ordering[Double].on(distRow => distRow._2))
        .map(distRow => (distRow._1, 1)).groupBy(_._1) //Pra cada grupo fazer o reduce e pegar o maior
      )
    )

    sc.stop()
  }

  def distance(trainRow: Array[Double], testRow: Array[Double]): Array[Double] = {
    trainRow.zip(testRow).map { case (x, y) => scala.math.pow(x - y, 2) }
  }
}
