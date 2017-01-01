package com.bhnte.spark

import com.bhnte.spark.format.csv.SparkCSVFileFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by b.hanotte on 09/11/16.
  */
object SparkTestBase {

  val spark = SparkSession
                .builder()
                .master("local")
                .appName("Spark unit tests")
                .getOrCreate()
}
