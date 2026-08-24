package amalgam.export

import org.apache.spark.sql.DataFrame

/** TSV report export (conflicts.tsv, merges.tsv): Spark CSV writer with tab delimiter into a temp dir, then
  * header-aware concatenation of the part files into one TSV on the driver.
  */
object Tsv {

  def writeSingleFile(df: DataFrame, destFile: os.Path): Unit = {
    val tmpDir = destFile / os.up / (destFile.last.stripSuffix(".tsv") + ".tsv.parts")
    os.remove.all(tmpDir)
    os.makeDir.all(destFile / os.up)

    df.write
      .mode("overwrite")
      .option("sep", "\t")
      .option("header", "true")
      .option("emptyValue", "")
      .csv(tmpDir.toString)

    val parts = os
      .list(tmpDir)
      .filter(p => p.last.startsWith("part-") && p.last.endsWith(".csv"))
      .sortBy(_.last)

    val out = new java.io.BufferedWriter(
      new java.io.OutputStreamWriter(java.nio.file.Files.newOutputStream(destFile.toNIO), "UTF-8")
    )
    try {
      parts.zipWithIndex.foreach { case (part, idx) =>
        val lines = os.read.lines(part)
        lines.zipWithIndex.foreach { case (line, lineNo) =>
          // header row only from the first part file
          if (idx == 0 || lineNo > 0) {
            out.write(line)
            out.newLine()
          }
        }
      }
    } finally out.close()

    os.remove.all(tmpDir)
  }
}
