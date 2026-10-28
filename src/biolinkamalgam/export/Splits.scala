package biolinkamalgam.export

/** Hugging Face split names. The hub requires `^\w+(\.\w+)*$`; we emit the stricter `[a-z][a-z0-9_]*` so a split name
  * is also a safe file name and survives Spark partition-directory escaping unchanged.
  *
  * Biolink classes drop the prefix and go CamelCase to snake_case (`biolink:SmallMolecule` -> `small_molecule`,
  * `biolink:RNAProduct` -> `rna_product`). Anything else (crossref sources) lowercases with non-alphanumeric runs as
  * `_` (`obo:HsapDv` -> `obo_hsapdv`).
  */
object Splits {
  private val Valid = "[a-z][a-z0-9_]*"

  def isValid(name: String): Boolean = name != null && name.matches(Valid)

  def name(value: String): String = {
    require(value != null && value.trim.nonEmpty, "split source value must be non-empty")
    val v = value.trim
    val cased =
      if (v.startsWith("biolink:"))
        v.stripPrefix("biolink:")
          .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
          .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
      else v
    val snake = cased
      .toLowerCase(java.util.Locale.ROOT)
      .replaceAll("[^a-z0-9]+", "_")
      .replaceAll("^_+|_+$", "")
    require(isValid(snake), s"cannot derive a split name from: $value")
    snake
  }

  /** Value -> split name for every value; fails loudly when two values would share one split. */
  def mapping(values: Seq[String]): Map[String, String] = {
    val m = values.distinct.map(v => v -> name(v)).toMap
    val clashes = m.groupBy(_._2).filter(_._2.size > 1).map { case (s, vs) => s"$s <- ${vs.keys.toSeq.sorted}" }
    require(clashes.isEmpty, s"values collide on split names: ${clashes.mkString(", ")}")
    m
  }
}
