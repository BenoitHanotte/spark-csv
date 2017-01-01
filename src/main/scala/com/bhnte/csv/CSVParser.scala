package com.bhnte.csv

import java.io.IOException

import org.apache.hadoop.io.Text

import scala.collection.mutable

class CSVParser {
  val stack = new mutable.Stack[State]
  stack.push(TrState())

  var cur = new java.lang.StringBuilder()
  val elms = collection.mutable.ArrayBuffer[String]()

  def isCompleteState: Boolean = stack.top.isCompleteState

  /**
    * Only used for easy testing or compatibility
    * @param s
    */
  def parse(s: String): Unit = {
    val buffer = StringBuffer(s)
    parse(buffer)
  }

  /**
    * Used for parsing lines of files read from the input format, used for high perfs
    * Returns true if reached a consistent state at the end of the line
    */
  def parse(text: Text): Unit = {
    val buffer = TextBuffer(text)
    parse(buffer)
  }

  protected def parse(buffer: Buffer): Unit = {
    var i = 0
    while (!buffer.isEmpty) {
      if (buffer.get == '\n') throw new IOException("Invalid new line char at ${buffer.getPos}: '${buffer.getContext}'")
      stack.top.parse(buffer)
    }
    isCompleteState // should only be transition state (the top level state), otherwise, it is not finished
  }

  def end() = {
    if (elms.nonEmpty || cur.length() > 0) flush()
    if (!isCompleteState) throw new IOException("Incomplete state when calling end(): the input is not parsable")
    elms.toArray
  }

  def flush() = {
    elms.append(cur.toString)
    cur = new java.lang.StringBuilder()
  }

  abstract class State(val name: String) {
    // return true if "complete" state (meaning we can stop parsing at this position of the buffer
    // and still have a valid csv
    def parse(buffer: Buffer) : Unit

    def isCompleteState: Boolean
  }

  case class TrState() extends State("TrState") {
    override def parse(buffer: Buffer) = {
      if (buffer.startsWith(",")) {
        buffer.inc(1)
        flush()
      }
      else if (buffer.startsWith("\"")) {
        stack.push(StringState())
        buffer.inc(1)
      }
      else {
        stack.push(ValState())
      }
    }

    override def isCompleteState: Boolean = true
  }

  case class StringState() extends State("StringState") {
    override def parse(buffer: Buffer) = {
      if (buffer.startsWith("\"\"")) {
        cur.append("\"")
        buffer.inc(2)
      }
      else if (buffer.startsWith("\",")) {
        flush()
        stack.pop
        buffer.inc(2)
      }
      else if (buffer.startsWith("\"")) {
        buffer.inc(1)
        if (buffer.isEmpty) {
          stack.pop
        } // end of line
        // invalid end of string in line (not the end of the line, not a transition to another value
        else throw new IOException(s"Invalid end of string at ${buffer.getPos}: '${buffer.getContext}'")
      }
      else {
        cur.append(buffer.get)
        buffer.inc(1)
      }
    }

    // not a complete state: if input ends in a string state, the csv is not complete and can not be parsed
    override def isCompleteState: Boolean = false
  }

  case class ValState() extends State("ValState") {
    override def parse(buffer: Buffer) = {
      if (buffer.startsWith(",")) {
        flush()
        stack.pop
        buffer.inc(1)
      }
      else if (buffer.startsWith("\"")) {
        throw new IOException(s"Invalid quote at ${buffer.getPos}: '${buffer.getContext}'")
      }
      else {
        cur.append(buffer.get)
        buffer.inc(1)
      }
    }

    override def isCompleteState: Boolean = true
  }

  abstract class Buffer(val len: Int) {
    protected var pos = 0 // the pos to consider

    def charAt(i: Int): Char

    def inc(i: Int) = {
      pos = pos + i
      this
    }

    def get = charAt(pos)

    def getPos = pos

    def size = pos - len

    def isEmpty = pos == len

    def getContext = {
      val sb = new java.lang.StringBuilder()
      for (i <- Math.max(0, pos - 5) until  Math.min(len, pos + 10)) sb.append(charAt(i))
      sb.toString
    }

    override def toString: String = {
      val sb = new java.lang.StringBuilder()
      for (i <- pos until len) sb.append(charAt(i))
      sb.toString
    }

    def startsWith(s: String): Boolean = {
      if (s.length > len-pos) return false
      val carr = s.toCharArray
      for (i <- carr.indices) if (carr(i) != charAt(pos+i)) return false
      true
    }
  }

  case class TextBuffer(text: Text) extends Buffer(text.getLength) {
    override def charAt(i: Int): Char = text.charAt(i).toChar
  }

  case class StringBuffer(string: String) extends Buffer(string.length) {
    override def charAt(i: Int): Char = string.charAt(i)
  }
}
