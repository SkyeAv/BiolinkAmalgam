package biolinkamalgam.export

/** Run manifest: per-stage row counts and durations, one JSON file per run. Hand-rolled minimal JSON — the shape is
  * flat and known, so no dependency is warranted.
  */
object Manifest {

  case class Stage(name: String, rows: Long, millis: Long)

  private def esc(s: String): String =
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\t' => "\\t"
      case c    => c.toString
    }

  def write(destFile: os.Path, stages: Seq[Stage], extra: Map[String, String] = Map.empty): Unit = {
    os.makeDir.all(destFile / os.up)
    val stageJson = stages
      .map(s => s"""    {"name": "${esc(s.name)}", "rows": ${s.rows}, "millis": ${s.millis}}""")
      .mkString(",\n")
    val extraJson = extra
      .map { case (k, v) => s"""    "${esc(k)}": "${esc(v)}"""" }
      .mkString(",\n")
    val content =
      s"""{
         |  "stages": [
         |$stageJson
         |  ],
         |  "extra": {
         |$extraJson
         |  }
         |}
         |""".stripMargin
    os.write.over(destFile, content)
  }
}
