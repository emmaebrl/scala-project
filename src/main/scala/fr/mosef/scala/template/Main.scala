package fr.mosef.scala.template

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem

import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.reader.schemas.CsvSchemas
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.writer.Writer

import java.io.FileInputStream
import java.util.Properties

object Main extends App {

  val cliArgs = args

  val MASTER_URL: String = try cliArgs(0) catch {
    case _: ArrayIndexOutOfBoundsException => "local[1]"
  }

  val SRC_PATH: String = try cliArgs(1) catch {
    case _: ArrayIndexOutOfBoundsException =>
      println("❌ Aucun chemin source défini")
      sys.exit(1)
  }

  val DST_PATH: String = try cliArgs(2) catch {
    case _: ArrayIndexOutOfBoundsException => "./default/output-writer"
  }

  val REPORT_TYPES: Seq[String] = try cliArgs(3).split(",").map(_.trim).toSeq catch {
    case _: ArrayIndexOutOfBoundsException =>
      println("ℹ️ Aucun type de rapport précisé, 'report1' utilisé par défaut")
      Seq("report1")
  }

  val CONFIG_PATH: Option[String] = if (cliArgs.length > 4) Some(cliArgs(4)) else None
  val HAS_HEADER: Boolean = try cliArgs(5).toBoolean catch {
    case _: Throwable => true
  }

  val conf = new SparkConf()
  conf.set("spark.driver.memory", "2g")
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.sparkContext.hadoopConfiguration.setClass(
    "fs.file.impl",
    classOf[BareLocalFileSystem],
    classOf[FileSystem]
  )

  def detectFormatFromPath(path: String): String = {
    val lower = path.toLowerCase
    if (lower.endsWith(".csv")) "csv"
    else if (lower.endsWith(".parquet")) "parquet"
    else if (lower.startsWith("hive:")) "hive"
    else "unknown"
  }

  val confWriter = new Properties()
  val stream = CONFIG_PATH match {
    case Some(path) if path.trim.nonEmpty =>
      println(s"📄 Chargement config externe : $path")
      new FileInputStream(path)
    case _ =>
      println("📄 Chargement config interne : application.properties")
      getClass.getClassLoader.getResourceAsStream("application.properties")
  }

  if (stream == null) {
    throw new RuntimeException("❌ Fichier de configuration introuvable")
  }

  confWriter.load(stream)

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer(sparkSession, confWriter)

  val format = detectFormatFromPath(SRC_PATH)
  println(s"📂 Format détecté : $format")

  val inputDF: DataFrame = format match {
    case "csv" =>
      reader.readCSV(SRC_PATH, delimiter = ",", header = HAS_HEADER, schema = Some(CsvSchemas.rappelSchema))
    case "parquet" =>
      reader.readParquet(SRC_PATH)
    case "hive" =>
      val tableName = SRC_PATH.stripPrefix("hive:")
      reader.readHiveTable(tableName)
    case _ =>
      println(s"❌ Format inconnu pour le chemin : $SRC_PATH")
      sys.exit(1)
  }

  REPORT_TYPES.foreach { report =>
    val processedDF = processor.process(inputDF, report)
    val outputPath = s"$DST_PATH/$report"
    println(s"📝 Écriture du rapport '$report' vers $outputPath")

    val cleanedDF = processedDF.columns.foldLeft(processedDF) { (df, col) =>
      df.withColumn(col, org.apache.spark.sql.functions.regexp_replace(df(col), "\r\n|\n|\r", " "))
    }

    writer.write(cleanedDF, outputPath)
  }
}
