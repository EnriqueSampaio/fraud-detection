package com.enriquesampaio.frauddetection.machinelearning

import io.jvm.uuid._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class KNN(private val k: Int, private val trainPath: String, private val testPath: String) {
  def train(sc: SparkContext): String = {

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

    val t_positive = sc.accumulator(0, "True Positive")
    val t_negative = sc.accumulator(0, "True Negative")
    val f_positive = sc.accumulator(0, "False Positive")
    val f_negative = sc.accumulator(0, "False Negative")

    val accuracy = results.map { result =>
      if (result._1 == result._2) {
        if (result._1 == "1") {
          t_positive+= 1
        } else {
          t_negative+= 1
        }
        1
      } else {
        if (result._1 == "1") {
          f_positive+= 1
        } else {
          f_negative+= 1
        }
        0
      }
    }.reduce(_+_).toDouble / results.count()

    val precision = t_positive.value / (t_positive.value + f_positive.value)
    val recall = t_positive.value / (t_positive.value + f_negative.value)
    val f_measure = 2 * ((precision * recall) / (precision + recall))

    trainSample.destroy()

    s"""Accuracy: $accuracy
       |Precision: $precision
       |Recall: $recall
       |F-Measure: $f_measure
     """.stripMargin
  }
}
