package com.bhnte.hadoop.format

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{InputFormat, RecordReader, TaskAttemptContext, TaskAttemptID}
import org.apache.hadoop.util.ReflectionUtils
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by b.hanotte on 09/11/16.
  */
class CSVInputFormatSuite extends FunSuite {

  val conf = new Configuration(false)
  conf.set("fs.default.name", "file:///")

  val inputFormat = ReflectionUtils.newInstance(classOf[CSVInputFormat[LongWritable, Array]].asInstanceOf[Class[InputFormat[LongWritable, ArrayBuffer[String]]]], conf)
  val context: TaskAttemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID())

  test("Should open a one-split csv file with one-line csv") {

    val testFile: File = new File(getClass.getClassLoader.getResource("csv/format/nosplits_onelines.csv").getPath)
    val path: Path = new Path(testFile.getAbsoluteFile.toURI)
    val split: FileSplit = new FileSplit(path, 0, testFile.length(), null)
    val reader = inputFormat.createRecordReader(split, context)
    reader.initialize(split, context)

    readSplit(reader) shouldBe ArrayBuffer(
      ArrayBuffer("12", "cde", "56", "78"),
      ArrayBuffer("ab", "cd", "ef", ""),
      ArrayBuffer("", "12", "ab", "cd"),
      ArrayBuffer("abc", "", "abc", "123")
    )
  }

  test("Should open a one-split csv file with two-lines csv") {

    val testFile: File = new File(getClass.getClassLoader.getResource("csv/format/nosplits_twolines.csv").getPath)
    val path: Path = new Path(testFile.getAbsoluteFile.toURI)
    val split: FileSplit = new FileSplit(path, 0, testFile.length(), null)
    val reader = inputFormat.createRecordReader(split, context)
    reader.initialize(split, context)

    readSplit(reader) shouldBe ArrayBuffer(
      ArrayBuffer("12", "cde", "56", "78"),
      ArrayBuffer("ab", "cd", "ef", ""),
      ArrayBuffer("", "12", "ab", "cd"),
      ArrayBuffer("abc", "", "abc", "123")
    )
  }

  test("Should open a one-split csv file with multi-lines csv") {

    val testFile: File = new File(getClass.getClassLoader.getResource("csv/format/nosplits_multilines.csv").getPath)
    val path: Path = new Path(testFile.getAbsoluteFile.toURI)
    val split: FileSplit = new FileSplit(path, 0, testFile.length(), null)
    val reader = inputFormat.createRecordReader(split, context)
    reader.initialize(split, context)

    readSplit(reader) shouldBe ArrayBuffer(
      ArrayBuffer("12", "cde", "56", "78"),
      ArrayBuffer("ab", "cd", "ef", ""),
      ArrayBuffer("", "12", "ab", "cd"),
      ArrayBuffer("abc", "", "abc", "123")
    )
  }

  test("Should open a two-splits csv file with multi-lines csv") {

    val testFile: File = new File(getClass.getClassLoader.getResource("csv/format/twosplits_multilines.csv").getPath)
    val path: Path = new Path(testFile.getAbsoluteFile.toURI)

    val split1: FileSplit = new FileSplit(path, 0, 21, null)  // cut at the end of 3rd line
    val reader1 = inputFormat.createRecordReader(split1, context)
    reader1.initialize(split1, context)

    val split2: FileSplit = new FileSplit(path, 22, testFile.length()-22, null)
    val reader2 = inputFormat.createRecordReader(split2, context)
    reader2.initialize(split2, context)

    readSplit(reader1) shouldBe ArrayBuffer(
      ArrayBuffer("12", "cde", "56", "78"),
      ArrayBuffer("ab", "cd", "ef", "")
    )

    /*readSplit(reader2) shouldBe ArrayBuffer(
      ArrayBuffer("", "12", "ab", "cd"),
      ArrayBuffer("abc", "", "abc", "123")
    )*/
  }

  test("Should read a multilne csv with multiple lines after split") {

    val testFile: File = new File(getClass.getClassLoader.getResource("csv/format/twosplits_multilines_after_split.csv").getPath)
    val path: Path = new Path(testFile.getAbsoluteFile.toURI)

    val split1: FileSplit = new FileSplit(path, 0, 20, null)
    val reader1 = inputFormat.createRecordReader(split1, context)
    reader1.initialize(split1, context)

    readSplit(reader1) shouldBe ArrayBuffer(
      ArrayBuffer("12", "cde", "56", "78"),
      ArrayBuffer("ab", "cdefghi", "ef", "")
    )
  }

  test("Should open a two-splits csv file with multi-lines csv 2") {

    val testFile: File = new File(getClass.getClassLoader.getResource("csv/format/twosplits_multilines_2.csv").getPath)
    val path: Path = new Path(testFile.getAbsoluteFile.toURI)

    val split1: FileSplit = new FileSplit(path, 0, 21, null)  // cut at the end of 3rd line
    val reader1 = inputFormat.createRecordReader(split1, context)
    reader1.initialize(split1, context)

    val split2: FileSplit = new FileSplit(path, 22, testFile.length()-22, null)
    val reader2 = inputFormat.createRecordReader(split1, context)
    reader2.initialize(split2, context)

    readSplit(reader1) shouldBe ArrayBuffer(
      ArrayBuffer("12", "cde", "56", "78"),
      ArrayBuffer("ab", "cd", "ef", "")
    )

    readSplit(reader2) shouldBe ArrayBuffer(
      ArrayBuffer("", "12", "ab", "cd"),
      ArrayBuffer("abc", "", "abc", "123")
    )
  }

  test("Should skip incorrect input in lines") {

    val testFile: File = new File(getClass.getClassLoader.getResource("csv/format/twosplits_multilines_error2.csv").getPath)
    val path: Path = new Path(testFile.getAbsoluteFile.toURI)

    val split1: FileSplit = new FileSplit(path, 0, 20, null)  // cut at the end of 3rd line
    val reader1 = inputFormat.createRecordReader(split1, context)
    reader1.initialize(split1, context)

    val split2: FileSplit = new FileSplit(path, 21, testFile.length()-21, null)
    val reader2 = inputFormat.createRecordReader(split1, context)
    reader2.initialize(split2, context)

    /*readSplit(reader1) shouldBe ArrayBuffer(
      ArrayBuffer("12", "cde", "56", "78"),
      ArrayBuffer("ab", "cd", "ef", "")
    )*/

    readSplit(reader2) shouldBe ArrayBuffer(
      ArrayBuffer("", "12", "ab", "cd"),
      ArrayBuffer("abc", "", "abc", "123")
    )
  }

  private def readSplit(reader: RecordReader[LongWritable, ArrayBuffer[String]]) = {
    val res: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()
    while (reader.nextKeyValue()) {
      res.append(reader.getCurrentValue)
    }
    res
  }

  private implicit class ArrayBufferEqual[V](first: mutable.Buffer[V]) {
    implicit def shouldBe(second: mutable.Buffer[V]): Unit = first.zip(second).foreach((v) => v._1 shouldBe v._2)
  }
}
