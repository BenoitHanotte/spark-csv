package com.bhnte.spark.format.csv

case class CSVOptions(@transient options: Map[String, String] = Map()) extends Serializable {
  val headerFlag: Boolean = options.getOrElse("header", true) == true
}
