package com.enriquesampaio.frauddetection

import java.io.{File, IOException, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext

package object util {
  def clean(filepath: String): Unit ={
    try {
      FileUtils.deleteDirectory(new File(filepath))
    } catch {
      case ioe: IOException => ioe.printStackTrace()
      case se: SecurityException => se.printStackTrace()
    }
  }

  def saveAsRdd(inputFilepath: String, sc: SparkContext): Unit = {
    clean("output/rdd_dataset")
    sc.textFile(inputFilepath)
      .map(row => row.split(","))
      .map(row => row(30) + "," + row.slice(0,30).mkString(",")).saveAsTextFile("output/rdd_dataset")
  }

  def normalize(sc: SparkContext): Unit = {
    clean("output/normalized")
    val rows = sc.textFile("output/rdd_dataset")
        .map(row => row.split(","))
        .map(row => (row(0), row.slice(1, 30).map(feature => feature.toDouble)))

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
    ).map(row => row._1 + "," + row._2.mkString(",")).saveAsTextFile("output/normalized")
  }

  def stratify(trainProp: Double, sc: SparkContext): Unit = {
    clean("output/stratified_train")
    clean("output/stratified_test")
    val rows = sc.textFile("output/normalized")
      .map(row => row.split(","))
      .map(row => (row(0), row.slice(1,30).map(feature => feature.toDouble)))

    val negatives = rows.filter(row => row._1.equals("0")).randomSplit(Array(trainProp, 1 - trainProp))
    val positives = rows.filter(row => row._1.equals("1")).randomSplit(Array(trainProp, 1 - trainProp))

    val train = negatives(0).union(positives(0)).map(row => row._1 + "," + row._2.mkString(",")).saveAsTextFile("output/stratified_train")
    val test = negatives(1).union(positives(1)).map(row => row._1 + "," + row._2.mkString(",")).saveAsTextFile("output/stratified_test")
  }
}
