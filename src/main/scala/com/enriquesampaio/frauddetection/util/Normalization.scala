package com.enriquesampaio.frauddetection.util

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext

class Normalization(private val inputFilepath: String, private val outputFilepath: String) {
  def normalize(sc: SparkContext): Unit = {
    val rows = sc.textFile(inputFilepath)
      .map(row => row.split(","))
      .map(row => (row(30)(1), row.slice(0,30).map(value => value.toDouble)))
    val count = rows.count()

    val means = rows.map(row => row._2).reduce{ case (x, y) =>
      x.zip(y).map { case (x, y) => x + y }
    }.map(featureSum => featureSum / count)

    val stddevs = rows.map(row => row._2).map(row =>
      row.zip(means).map{ case (feature, mean) => scala.math.pow(feature - mean, 2) }
    ).reduce{ case (x, y) =>
      x.zip(y).map { case (x, y) => x + y }
    }.map(dev => scala.math.sqrt(dev / count))

    val rowsNorm = rows.map(row =>
      (row._1, row._2.zip(means).map{ case(feature, mean) => feature - mean }.zip(stddevs).map{ case(stage, stddev) => stage / stddev })
    ).collect()

    val pw = new PrintWriter(new File(outputFilepath))

    rowsNorm.map(row => row._2.mkString(",") + "," + row._1).foreach(row => pw.println(row))
    pw.close()
  }
}
