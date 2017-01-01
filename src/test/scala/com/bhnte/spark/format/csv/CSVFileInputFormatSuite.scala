package com.bhnte.spark.format.csv

import com.bhnte.spark.SparkTestBase
import org.scalatest.FunSuite
import org.scalatest.Matchers._

/**
  * Created by b.hanotte on 13/11/16.
  */
class CSVFileInputFormatSuite extends FunSuite {
  import SparkTestBase._

  test("work") {

    val path = getClass.getClassLoader.getResource("csv/format/nosplits_onelines.csv").getPath

    val df = spark
      .read
      .format("com.bhnte.spark.format.csv.BetterCSVFileFormat")
      .load(path)

    df.show

    df.count shouldBe 4L
  }


}
