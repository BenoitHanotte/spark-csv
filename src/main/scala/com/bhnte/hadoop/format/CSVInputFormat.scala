package com.bhnte.hadoop.format

import java.io.IOException

import com.bhnte.hadoop.parser.CSVParser
import com.bhnte.instrumentation.Instrument
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.util.LineReader

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by b.hanotte on 07/11/16.
  */
class CSVInputFormat[LongWritable, ArrayBuffer[String]] extends FileInputFormat[LongWritable, ArrayBuffer[String]] {
  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): RecordReader[LongWritable, ArrayBuffer[String]] =
    new CSVRecordReader(Some("\n".getBytes)).asInstanceOf[RecordReader[LongWritable, ArrayBuffer[String]]]
}

object CSVRecordReader {
  private val Log = LogFactory.getLog(getClass)
}

class CSVRecordReader(val recordDelimiter: Option[Array[Byte]] = None) extends RecordReader[LongWritable, ArrayBuffer[String]] {
  import CSVRecordReader._

  private var start: Long = 0L
  private var pos: Long = 0L
  private var end = 0L

  private var isCompressedInput: Boolean = _
  private var decompressor: Option[Decompressor] = _
  private var in: LineReader = _
  private var filePosition: Seekable = _
  private var maxLineLength: Int = Int.MaxValue

  private var key: LongWritable = new LongWritable()
  private val splitFirstKey = key
  private var textLine: Text = new Text()
  private var value: ArrayBuffer[String] = _

  override def initialize(genericSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val job = context.getConfiguration
    val split = genericSplit.asInstanceOf[FileSplit]
    val file = split.getPath
    val fs = file.getFileSystem(job)
    val fileIn = fs.open(file)
    val codec = Option(new CompressionCodecFactory(job).getCodec(file))

    start = split.getStart
    end = start + split.getLength

    codec match {
      case Some(codec1) => {
        isCompressedInput = true
        decompressor = Some(CodecPool.getDecompressor(codec1))

        codec1 match {
          case codec2: SplittableCompressionCodec => {
            val cIn = codec.asInstanceOf[SplittableCompressionCodec].createInputStream(fileIn, decompressor.get, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK)
            start = cIn.getAdjustedStart
            end = cIn.getAdjustedEnd
            filePosition = cIn
            recordDelimiter match {
              case Some(d) =>  in = new LineReader(cIn, job, recordDelimiter.get)
              case None => in = new LineReader(cIn, job)
            }
          }
          case codec2 => {
            val cIn = codec2.createInputStream(fileIn, decompressor.get)
            filePosition = fileIn
            recordDelimiter match {
              case Some(d) =>  in = new LineReader(cIn, job, recordDelimiter.get)
              case None => in = new LineReader(cIn, job)
            }
          }
        }
      }
      case None => {
        fileIn.seek(start)
        recordDelimiter match {
          case Some(d) =>  in = new LineReader(fileIn, job, recordDelimiter.get)
          case None => in = new LineReader(fileIn, job)
        }
      }
    }

    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) start += in.readLine(new Text(), 0, maxBytesToConsume(start))
    this.pos = start
  }

  private def maxBytesToConsume(pos: Long): Int =
    if (isCompressedInput) Integer.MAX_VALUE
    else Math.min(Integer.MAX_VALUE, end - pos).toInt

  @throws[IOException]
  private def getFilePosition: Long = {
    var retVal: Long = 0L
    if (isCompressedInput && null != filePosition) retVal = filePosition.getPos
    else retVal = pos
    retVal
  }

  override def getProgress: Float =
    if (start == end) 0.0f
    else Math.min(1.0f, (getFilePosition - start) / (end - start).toFloat)

  override def nextKeyValue(): Boolean = {
    var isFirstLineNotFirstSplit = pos > 0 && key == splitFirstKey

    // Cache next key value
    // CAUTION: the first line can be the end of the previous split and thus be a non-parsable line!
    // We always read at least one extra line, which lies outside the upper
    // split limit i.e. (end - 1)

    val lineBuffer = new LineBuffer()

    // queue used to skip invalid lines (eg: when beginning of the split is the end of the previous one)
    val posQueue = mutable.Queue[Long](pos)

    while (posQueue.nonEmpty && getFilePosition < end) {
      val startPos = posQueue.dequeue()

      try {
        val (nextStartPos, parsed) = parseFromLine(startPos, posQueue, lineBuffer)
        // if we reach this point, parsing was successful
        pos = nextStartPos
        key = new LongWritable(startPos)
        value = parsed
        return true
      }
      catch {
        // if first line of the split and throw IOException: it is the last lines of the previous split's that overflow, don't throw it out
        case ex: IOException if isFirstLineNotFirstSplit => Log.debug("Split's start was not parsable, trying to start next line", ex)
        // other wise: log error
        case ex: IOException => Log.error(s"CSVParser threw exception for invalid input for csv starting pos $startPos", ex)
        case ex => throw ex
      }
    }

    key = null
    value = null
    false
  }

  /**
    * If can't parse a valid CSV, will throw IOException
    * @param startPos
    * @return a tuple of:
    *         1. The position of the next record's start
    *         2. The parsed CSV values
    */
  private def parseFromLine(startPos: Long, posQueue: mutable.Queue[Long], lineBuffer: LineBuffer): (Long, ArrayBuffer[String]) = {
    val parser = new CSVParser()
    var isValidCsvParsed = false
    var curPos = startPos

    while (getFilePosition <= end && !isValidCsvParsed) {
      // read line
      val (newSize, text) = readBufferedLine(curPos, lineBuffer)
      if (newSize == 0) throw new IOException(s"Invalid csv  starting at pos ${startPos} extending to end of file")
      curPos += newSize // curPos is now the position of the next line
      posQueue.enqueue(curPos)
      // parse the line. if we reach a complete state at the end of the line: the csv is complete
      // raises an exception if incorrect input
      parser.parse(text)
      isValidCsvParsed = parser.isCompleteState
    }
    // will only reach this state if no exception raised during parsing
    (curPos, parser.end())
  }

  private def readBufferedLine(pos: Long, lineBuffer: LineBuffer): (Int, Text) = {
    lineBuffer.get(pos) match {
      case Some(v) => v
      case None =>
        val text = lineBuffer.preempt(pos)
        val newSize = in.readLine(text, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength))
        val tuple = (newSize, text)
        lineBuffer.put(pos, tuple)
        tuple
    }
  }

  override def getCurrentValue = value

  override def getCurrentKey = key

  @throws[IOException]
  def close() {
    synchronized {
      try {
        if (in != null) in.close()
      }
      finally {
        if (decompressor.nonEmpty) CodecPool.returnDecompressor(decompressor.get)
      }
    }
  }

  class LineBuffer {
    val map = mutable.Map[Long, (Int, Text)]()

    def preempt(pos: Long) = {
      val t = new Text()
      map.put(pos, (0, t))
      t
    }

    def put(pos: Long, tuple: (Int, Text)) = map.put(pos, tuple)

    def get(pos: Long) = map.get(pos)
  }
}