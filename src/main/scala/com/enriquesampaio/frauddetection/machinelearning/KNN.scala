package com.enriquesampaio.frauddetection.machinelearning

import io.jvm.uuid._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class KNN(private val k: Int, private val trainPath: String, private val testPath: String) {
  def train(sc: SparkContext): Double = {

    val testSample = sc.textFile(testPath).map(row => row.split(",")).map(row => (row(30), row.slice(0,30).map(feature => feature.toDouble)))
    val trainSample = sc.broadcast(sc.textFile(trainPath).map(row => row.split(",")).map(row => (row(30), row.slice(0,30).map(feature => feature.toDouble))).collect())

    val neighbours = sc.broadcast(k)

    val results = testSample
      .map(testRow => (
        testRow._1,
        trainSample.value
            .map(trainRow => (trainRow._1, scala.math.sqrt(trainRow._2.zip(testRow._2).map { case (x, y) => scala.math.pow(x - y, 2) }.sum) ))
            .sortBy(_._2)
            .take(neighbours.value)
            .map(distRow => (distRow._1, 1)).groupBy(_._1)
            .map(label => (label._1, label._2.foldLeft(0)((groupedA, groupedB) => groupedA + groupedB._2)))
            .maxBy(_._2)._1
      )
    )

    val accuracy = results.map { result =>
      if (result._1 == result._2) {
        1
      } else {
        0
      }
    }.reduce(_+_).toDouble / results.count()

    trainSample.destroy()

    accuracy
  }
}
