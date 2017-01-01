package com.bhnte.spark.format.csv

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.URI

import com.bhnte.hadoop.format.{CSVInputFormat, CSVRecordReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by b.hanotte on 12/11/16.
  */
class SparkCSVFileFormat extends TextBasedFileFormat with DataSourceRegister with Serializable {

  override def shortName(): String = "spark-csv"

  override def toString: String = "Spark-CSV"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[CSVFileFormat]

  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    val csvOptions = new CSVOptions(options)

    val paths = files.filterNot(_.getPath.getName startsWith "_").map(_.getPath.toString)
    val rdd = baseRdd(sparkSession, csvOptions, paths)

    val firstRow = findFirstLine(csvOptions, rdd)

    val header = if (csvOptions.headerFlag) {
      firstRow.zipWithIndex.map { case (value, index) =>
        if (value == null || value.isEmpty) s"_c$index" else value
      }
    } else {
      firstRow.zipWithIndex.map { case (value, index) => s"_c$index" }
    }

    val schema = new StructType(firstRow.map(name => StructField(name, StringType)).toArray)
    Some(schema)
  }

  override def buildReader(sparkSession: SparkSession,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val csvOptions = new CSVOptions(options)
    val headers = requiredSchema.fields.map(_.name)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      // create file split
      val fileSplit = new FileSplit(
        new Path(new URI(file.filePath)),
        file.start,
        file.length,
        Array.empty)

      val conf = broadcastedHadoopConf.value.value

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

      // instanciate our hadoop CSV record reader
      val reader = new CSVRecordReader()
      reader.initialize(fileSplit, hadoopAttemptContext)

      val recordIterator = {
        val iterator = new RecordReaderIterator(reader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iterator.close()))
        iterator
      }

      if (csvOptions.headerFlag && file.start == 0) {
        // drop header line
        recordIterator.next()
      }

      var numMalformedRecords = 0
      recordIterator.flatMap { record =>
        if (!fitSchema(requiredSchema, record)) {
          numMalformedRecords += 1
          None
        }
        Some(makeRow(record))
      }
    }
  }
  private def makeRow(record: Array[String]): InternalRow = {
    new GenericInternalRow(record.map(s=>UTF8String.fromString(s)).asInstanceOf[Array[Any]])
  }

  private def fitSchema(schema: StructType, record: Array[String]): Boolean = {
    schema.size == record.length
  }

  private def baseRdd(sparkSession: SparkSession,
                      options: CSVOptions,
                      inputPaths: Seq[String]): RDD[Array[String]] = {
    readText(sparkSession, options, inputPaths.mkString(","))
  }

  /**
    * Returns the first line of the first non-empty file in path
    */
  private def findFirstLine(options: CSVOptions, rdd: RDD[Array[String]]) = rdd.first()

  private def readText(sparkSession: SparkSession,
                       options: CSVOptions,
                       location: String): RDD[Array[String]] = {
    sparkSession.sparkContext
      .newAPIHadoopFile(location, classOf[CSVInputFormat[LongWritable, Array]], classOf[LongWritable], classOf[Array[String]])
      .map(_._2)
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    throw new NotImplementedError(s"prepareWrite not implemented in ${this}")
  }
}

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }
}
