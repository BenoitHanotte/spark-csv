package com.bhnte.instrumentation

/**
  * Created by b.hanotte on 11/11/16.
  */
object Instrument {

  def apply[OUT](name: String, additional: String = "")(fun: => OUT)(implicit _level: MutableInt = new MutableInt()): OUT = {

    _level.inc()

    val _instru_start = System.currentTimeMillis()
    printIntruIn(name, additional, _instru_start, _level.get)

    var out = fun

    val _instru_stop = System.currentTimeMillis()
    printIntruOut(name, _instru_start, _instru_stop, _level.get)

    _level.dec()

    out
  }

  private def printIntruIn(name: String, additional: String, startTime: Long, level: Int) = {
    var buf = new StringBuilder()
    for (i <- 1 until level) buf.append("--")
    buf.append(">>")
    buf.append(" ")
    buf.append(name)
    if (additional.nonEmpty) buf.append(" (").append(additional).append(")")
    println(buf.toString)
  }

  private def printIntruOut(name: String, startTime: Long, stopTime: Long, level: Int) = {
    var buf = new StringBuilder()
    for (i <- 1 until level) buf.append("--")
    buf.append("<< ").append(name)
    buf.append(": ").append(stopTime - startTime).append("ms")
    println(buf.toString)
  }

  class MutableInt(var value: Int = 0) {
    def inc() = { value = value + 1 }
    def dec() = { value = value - 1 }
    def get() = value
  }
}
