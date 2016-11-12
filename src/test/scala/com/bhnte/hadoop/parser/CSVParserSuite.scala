package com.bhnte.hadoop.parser

import java.io.IOException

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class CSVParserSuite extends FunSuite {

  test("CSVParser should parse values") {
    testMatch(Array(), "")
    testMatch(Array("123"), "123")
    testMatch(Array("123","4"), "123,4")
    testMatch(Array("1", "234"), "1,234")
    testMatch(Array("", "123", "4"), ",123,4")
    testMatch(Array("123", "4", ""), "123,4,")
    testMatch(Array("", "", "123", "4"), ",,123,4")
    testMatch(Array("123", "4", "", ""), "123,4,,")
    testMatch(Array("abc", "", "", "123", "4"), "abc,,,123,4")
    testMatch(Array("123", "4", "", "abc"), "123,4,,abc")
    testMatch(Array("123", "4", " ", "abc"), "123,4, ,abc")
  }

  test("CSVParser should parse strings") {
    testMatch(Array("abc"), """"abc"""")
    testMatch(Array("abc", "123"), """"abc",123""")
    testMatch(Array("abc", "123", "456"), """"abc",123,"456"""")
    testMatch(Array("", "abc"), ""","abc"""")
    testMatch(Array("", "abc", "123"), """"","abc",123""")
    testMatch(Array("abc", "123", "456", "", ""), """"abc",123,"456",,""""")
    testMatch(Array("abc", "123", "456", "", ""), """"abc",123,"456","",""")
  }

  test("CSVParser should parse strings with commas") {
    testMatch(Array("ab,c"), """"ab,c"""")
    testMatch(Array("abc,", "123"), """"abc,",123""")
  }

  test("CSVParser should parse json-like strings") {
    testMatch(Array("{abc}"), """"{abc}"""")
    testMatch(Array("{abc,\"de f\"}"), """"{abc,""de f""}"""")
    testMatch(Array("{abc,\"de f\"}", "123"), """"{abc,""de f""}",123""")
  }

  test("CSVParser should declare non-complete state") {
    testIncomplete(""""abc""")
    testIncomplete("""123,"abc""")
  }

  test("CSVParser should throw Exceptions on incorrect inputs") {
    testIOException("\"abc")
    testIOException("\"abc,123")
    testIOException("abc\",123")
    testIOException("123,\"")
    testIOException("\"a\"bc")
  }

  test("CSVParser should parse multiple lines") {
    testMatch(Array("abc", "123", "456", "", ""), """"abc",123,"4""", """56",,""""")
    testMatch(Array("abc", "123", "456", "", ""), """"ab""", """c",123,"456","",""")
  }
  
  private def testMatch(expected: Array[String], inputs: String*): Unit = {
    val p = new CSVParser()
    for (input <- inputs) {
      p.parse(input)
    }
    p.isCompleteState shouldBe true
    p.end() shouldBe expected
  }

  private def testIncomplete(s: String) {
    val p = new CSVParser()
    p.parse(s)
    p.isCompleteState shouldBe false
  }

  private def testIOException(s: String): Unit = {
    val p = new CSVParser()
    intercept[IOException] {
      p.parse(s)
      p.end()
    }
  }

}