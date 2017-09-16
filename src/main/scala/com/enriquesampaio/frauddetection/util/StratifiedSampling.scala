package com.enriquesampaio.frauddetection.util

import org.apache.spark.{SparkConf, SparkContext}

object StratifiedSampling {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Fraud Detection").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rows = sc.textFile("resources/creditcard.csv")
      .map(row => row.split(","))
      .map(row => (row(30)(1), row.slice(0,30)))

    val count = rows.count()
    val targetsPercentage = rows.map(row => (row._1, 1)).reduceByKey(_ + _).map(target => (target._1, target._2.toDouble / count))

    val trainSize = (70 / count).floor.toInt
    val testSize = count - trainSize

    sc.stop()
  }
}
