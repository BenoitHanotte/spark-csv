package com.bhnte.hadoop.parser

import com.bhnte.csv.CSVParser
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class CSVParserRealDataSuite extends FunSuite {

  test("Should parse the real-data line") {
    val parser = new CSVParser()
    val stream = getClass.getResourceAsStream("/csv/parser/3lines.csv")
    val lines = scala.io.Source.fromInputStream(stream).getLines

    lines.foreach(l=>parser.parse(l))

    parser.isCompleteState shouldBe true
    parser.end().size shouldBe 95
  }
}