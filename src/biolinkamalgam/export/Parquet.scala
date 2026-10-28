package biolinkamalgam.export

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.spark.sql.DataFrame

/** Parquet export: parallel partitioned write, then a binary row-block concat + footer rewrite (PARQUET-460 style, via
  * parquet4j's ParquetFileWriter.appendFile — no SerDe, pure I/O copy). Falls back to coalesce(1) if the parquet4j
  * append API is unavailable.
  */
object Parquet {

  /** Hadoop local files may carry a hidden checksum sibling; replacing data alone leaves stale checksums. */
  private def replaceFile(source: os.Path, destination: os.Path): Unit = {
    val sourceCrc = source / os.up / s".${source.last}.crc"
    val destinationCrc = destination / os.up / s".${destination.last}.crc"
    os.move(source, destination, replaceExisting = true)
    if (os.exists(sourceCrc)) os.move(sourceCrc, destinationCrc, replaceExisting = true)
    else os.remove(destinationCrc, checkExists = false)
  }

  /** Write `df` as ONE parquet file at `destFile` (zstd, sized row groups). */
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
      replaceFile(part, destFile)
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
      replaceFile(parts.head, destFile)
      os.remove.all(tmpDir)
      return
    }

    try {
      // Build beside the parts then replace: reruns must not fail CREATE against the existing destination,
      // and the previous deliverable remains intact until the complete footer has been written.
      val staged = tmpDir / "combined.parquet"
      binaryConcat(parts, staged)
      replaceFile(staged, destFile)
    } catch {
      case _: NoSuchMethodError | _: NoClassDefFoundError =>
        // parquet4j API drift — fall back to the single-writer path.
        os.remove.all(tmpDir)
        writeSingleFile(df, destFile, simple = true)
        return
    }
    os.remove.all(tmpDir)
  }

  /** Write `df` as ONE parquet file per distinct value of `splitColumn`: `destDir/<value>.parquet`. One partitioned
    * Spark pass, then the same binary concat per partition. The split column is dropped from the files (Spark keeps
    * partition values in directory names only). Values must already be safe file names (see [[Splits]]). Files are
    * replaced only once complete; files in `destDir` that are not part of this write are pruned afterwards. Returns the
    * written split names, sorted. With `sortBy`, each file is sorted by those columns: a global range sort numbers the
    * part files in key order, so concatenating them by name keeps the order.
    */
  def writePartitioned(
      df: DataFrame,
      splitColumn: String,
      destDir: os.Path,
      simple: Boolean = false,
      sortBy: Seq[String] = Seq.empty
  ): Seq[String] = {
    val tmpDir = destDir / os.up / s"${destDir.last}.parts"
    os.remove.all(tmpDir)
    val frame =
      if (simple) {
        val partitioned = df.repartition(df.col(splitColumn))
        if (sortBy.nonEmpty) partitioned.sortWithinPartitions((splitColumn +: sortBy).map(df.col): _*)
        else partitioned
      } else if (sortBy.nonEmpty) df.orderBy((splitColumn +: sortBy).map(df.col): _*)
      else df
    frame.write
      .mode("overwrite")
      .option("compression", "zstd")
      .partitionBy(splitColumn)
      .parquet(tmpDir.toString)

    val prefix = s"$splitColumn="
    val partitions = os.list(tmpDir).filter(p => os.isDir(p) && p.last.startsWith(prefix)).sortBy(_.last)
    val names = partitions.map(_.last.stripPrefix(prefix))
    names.foreach(n => require(Splits.isValid(n), s"unsafe split name from partition directory: $n"))
    os.makeDir.all(destDir)
    partitions.zip(names).foreach { case (dir, name) =>
      val parts = os.list(dir).filter(p => p.last.startsWith("part-") && p.last.endsWith(".parquet")).sortBy(_.last)
      require(parts.nonEmpty, s"partition without data files: $dir")
      val dest = destDir / s"$name.parquet"
      if (parts.length == 1) replaceFile(parts.head, dest)
      else {
        val staged = dir / "combined.parquet"
        try binaryConcat(parts, staged)
        catch {
          case _: NoSuchMethodError | _: NoClassDefFoundError =>
            os.remove(staged, checkExists = false)
            val schema = org.apache.spark.sql.types.StructType(df.schema.fields.filterNot(_.name == splitColumn))
            val data = df.sparkSession.read.schema(schema).parquet(parts.map(_.toString): _*)
            val sorted = if (sortBy.nonEmpty) data.orderBy(sortBy.map(data.col): _*) else data
            writeSingleFile(sorted, staged, simple = true)
        }
        replaceFile(staged, dest)
      }
    }
    val expected = names.flatMap(n => Seq(s"$n.parquet", s".$n.parquet.crc")).toSet
    os.list(destDir).filterNot(f => expected(f.last)).foreach(os.remove.all(_))
    os.remove.all(tmpDir)
    names
  }

  /** Row count from the parquet footer alone (no Spark, no row decoding). */
  def rowCount(file: os.Path): Long = {
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toString), new Configuration()))
    try reader.getRecordCount
    finally reader.close()
  }

  /** Concatenate parquet part files into one file without deserializing rows: appendFile copies each part's row blocks
    * verbatim and the writer rewrites the footer.
    */
  private def binaryConcat(parts: Seq[os.Path], destFile: os.Path): Unit = {
    val conf = new Configuration()
    val first = HadoopInputFile.fromPath(new Path(parts.head.toString), conf)
    val reader = ParquetFileReader.open(first)
    val footer =
      try reader.getFooter
      finally reader.close()
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
