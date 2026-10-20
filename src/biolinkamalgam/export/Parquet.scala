package biolinkamalgam.export

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.spark.sql.DataFrame

/** Parquet export: parallel partitioned write, then a binary row-block concat + footer rewrite (PARQUET-460 style, via
  * parquet4j's ParquetFileWriter.appendFile — no SerDe, pure I/O copy). Falls back to coalesce(1) if the parquet4j
  * append API is unavailable.
  */
object Parquet {

  /** Write `df` as ONE parquet file at `out/biolinkamalgam.parquet` (zstd, sized row groups). */
  def writeSingleFile(df: DataFrame, destFile: os.Path, simple: Boolean = false): Unit = {
    val tmpDir = destFile / os.up / (destFile.last.stripSuffix(".parquet") + ".parts")
    os.remove.all(tmpDir)
    os.makeDir.all(destFile / os.up)

    if (simple) {
      // Fallback path: single-writer Spark export, then rename the lone part file.
      df.coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "zstd")
        .parquet(tmpDir.toString)
      val part = os.list(tmpDir).filter(_.last.endsWith(".parquet")).head
      os.move(part, destFile, replaceExisting = true)
      os.remove.all(tmpDir)
      return
    }

    // Fast path: parallel partitioned write, then binary footer merge.
    df.write
      .mode("overwrite")
      .option("compression", "zstd")
      .parquet(tmpDir.toString)

    val parts = os
      .list(tmpDir)
      .filter(p => p.last.startsWith("part-") && p.last.endsWith(".parquet"))
      .sortBy(_.last)

    if (parts.length == 1) {
      os.move(parts.head, destFile, replaceExisting = true)
      os.remove.all(tmpDir)
      return
    }

    try {
      binaryConcat(parts, destFile)
    } catch {
      case _: NoSuchMethodError | _: NoClassDefFoundError =>
        // parquet4j API drift — fall back to the single-writer path.
        os.remove.all(tmpDir)
        writeSingleFile(df, destFile, simple = true)
        return
    }
    os.remove.all(tmpDir)
  }

  /** Concatenate parquet part files into one file without deserializing rows: appendFile copies each part's row blocks
    * verbatim and the writer rewrites the footer.
    */
  private def binaryConcat(parts: Seq[os.Path], destFile: os.Path): Unit = {
    val conf = new Configuration()
    val first = HadoopInputFile.fromPath(new Path(parts.head.toString), conf)
    val footer = ParquetFileReader.open(first).getFooter
    val schema = footer.getFileMetaData.getSchema
    val extraMeta = new java.util.HashMap[String, String]()
    footer.getFileMetaData.getKeyValueMetaData.forEach((k, v) => extraMeta.put(k, v))

    val out = HadoopOutputFile.fromPath(new Path(destFile.toString), conf)
    val writer =
      new ParquetFileWriter(
        out,
        schema,
        ParquetFileWriter.Mode.CREATE,
        ParquetWriterDefaults.RowGroupSize,
        ParquetWriterDefaults.MaxPaddingSize
      )
    writer.start()
    parts.foreach { p =>
      writer.appendFile(HadoopInputFile.fromPath(new Path(p.toString), conf))
    }
    writer.end(extraMeta)
  }

  private object ParquetWriterDefaults {
    val RowGroupSize: Long = 128L * 1024 * 1024 // 128MB row groups
    val MaxPaddingSize: Int = 8 * 1024 * 1024
  }
}
