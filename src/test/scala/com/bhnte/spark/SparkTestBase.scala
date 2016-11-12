package com.bhnte.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by b.hanotte on 09/11/16.
  */
object SparkTestBase {

  private var sc: Option[SparkContext] = _

  def getSparkContext = {
    synchronized {
      sc match {
        case None =>
          sc = Some(new SparkContext(new SparkConf().setMaster("local").setAppName("spark-csv-unit")))
          sc.get
        case _ => sc.get
      }
    }
  }
}
