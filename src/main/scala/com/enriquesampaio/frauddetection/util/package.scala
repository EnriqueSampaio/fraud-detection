package com.enriquesampaio.frauddetection

import java.io.{File, IOException, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

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

  def toLabeledPoints(rows: RDD[String]): RDD[LabeledPoint] = {
    rows
      .map(_.split(","))
      .map(row => LabeledPoint(row(0).toDouble, Vectors.dense(row.slice(1, 30).map(_.toDouble))))
  }

  def scale(sc: SparkContext): Unit = {
    clean("output/normalized")
    val rows = toLabeledPoints(sc.textFile("output/rdd_dataset"))

    val scaler = new StandardScaler(true, true).fit(rows.map(_.features))

    rows
      .map(row => (row.label, scaler.transform(row.features)))
      .map(row => row._1 + "," + row._2.toArray.mkString(","))
      .saveAsTextFile("output/scaled")
  }

  def stratify(trainProp: Double, sc: SparkContext): Unit = {
    clean("output/stratified_train")
    clean("output/stratified_test")

    val rows = toLabeledPoints(sc.textFile("output/normalized"))

    val negatives = rows.filter(row => row.label.equals(0.0)).randomSplit(Array(trainProp, 1 - trainProp))
    val positives = rows.filter(row => row.label.equals(1.0)).randomSplit(Array(trainProp, 1 - trainProp))

    negatives(0).union(positives(0)).map(row => row.label + "," + row.features.toArray.mkString(",")).saveAsTextFile("output/stratified_train")
    negatives(1).union(positives(1)).map(row => row.label + "," + row.features.toArray.mkString(",")).saveAsTextFile("output/stratified_test")
  }
}
