package fr.mosef.scala.template

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

import java.util.Properties
import java.io.FileInputStream

object Main extends App {

  val cliArgs = args // On récupère les arguments fournis par la commande line interface

  val MASTER_URL: String = try cliArgs(0) catch {
    case _: ArrayIndexOutOfBoundsException => "local[1]"
  }

  val SRC_PATH: String = try cliArgs(1) catch {
    case _: ArrayIndexOutOfBoundsException =>
      println("No input defined")
      sys.exit(1)
  } // Chemin des données d'entrée

  val DST_PATH: String = try cliArgs(2) catch {
    case _: ArrayIndexOutOfBoundsException => "./default/output-writer"
  } // Output

  val REPORT_TYPE: String = try cliArgs(3) catch {
    case _: ArrayIndexOutOfBoundsException =>
      println("Aucun type de rapport précisé, 'report1' utilisé par défaut")
      "report1"
  } // Quel rapport sur les 3 on génère (report1, report2 ou report3)

  val CONFIG_PATH: Option[String] = if (cliArgs.length > 4) Some(cliArgs(4)) else None

  val conf = new SparkConf()
  conf.set("spark.driver.memory", "64M") // ??
  conf.set("spark.testing.memory", "471859200") // ??

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
  ) // ??

  def detectFormatFromPath(path: String): String = { // On déduit automatiquement le format du fichier d'entrée
    val lower = path.toLowerCase
    if (lower.endsWith(".csv")) "csv"
    else if (lower.endsWith(".parquet")) "parquet"
    else if (lower.startsWith("hive:")) "hive"
    else "unknown"
  }

  // ✅ Chargement configuration depuis fichier interne ou externe
  val confWriter = new Properties()
  val stream = CONFIG_PATH match {
    case Some(path) =>
      println(s"📄 Chargement config externe : $path")
      new FileInputStream(path)
    case None =>
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
  println(s"Format détecté: $format")

  val inputDF: DataFrame = format match {
    case "csv" =>
      reader.readCSV(SRC_PATH, delimiter = ",", header = true)
    case "parquet" =>
      reader.readParquet(SRC_PATH)
    case "hive" =>
      val tableName = SRC_PATH.stripPrefix("hive:")
      reader.readHiveTable(tableName)
    case _ =>
      println(s"Format inconnu pour le chemin : $SRC_PATH")
      sys.exit(1)
  } // On appelle la bonne fonction read du Reader selon le type de fichier détécté

  val processedDF: DataFrame = processor.process(inputDF, REPORT_TYPE) // On appelle le process avec le REPORT_TYPE
  val outputPath = s"$DST_PATH/$REPORT_TYPE"
  println(s"Écriture du rapport '$REPORT_TYPE' vers $outputPath")
  writer.write(processedDF, outputPath) // Output
}
