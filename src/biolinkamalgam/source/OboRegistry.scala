package biolinkamalgam.source

import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._

/** Reviewed OBO ontology list (config/obo_sources.yaml). One OboSource per entry; unknown keys or a missing file fail
  * loudly instead of silently skipping an ontology.
  */
object OboRegistry {
  private val Allowed = Set("prefix", "url", "category", "license", "taxa")

  case class Entry(prefix: String, url: String, category: String, license: String, taxa: Seq[String]) {
    require(prefix.matches("[A-Za-z][A-Za-z0-9_-]*"), s"invalid OBO prefix: $prefix")
    require(url.startsWith("https://") || url.startsWith("http://"), s"invalid OBO URL: $url")
    require(category.matches("biolink:[A-Za-z]+"), s"invalid Biolink category: $category")
    require(license.nonEmpty && taxa.forall(_.matches("NCBITaxon:[1-9][0-9]*")), "invalid OBO license/taxa")
  }

  def entries(path: os.Path): Seq[Entry] = {
    require(os.exists(path), s"OBO sources config does not exist: $path")
    val root = new Yaml().load[java.util.Map[String, Object]](os.read(path))
    require(root != null && root.containsKey("ontologies"), s"OBO sources config has no ontologies list: $path")
    val list = root.get("ontologies").asInstanceOf[java.util.List[Object]].asScala
    val parsed = list.toSeq.map { raw =>
      val m = raw.asInstanceOf[java.util.Map[String, Object]].asScala
      require(m.keySet.forall(Allowed.contains), s"unknown OBO source keys: ${m.keySet.toSet -- Allowed}")
      Entry(
        prefix = str(m, "prefix", path),
        url = str(m, "url", path),
        category = str(m, "category", path),
        license = str(m, "license", path),
        taxa = m.get("taxa") match {
          case None                          => Seq.empty
          case Some(list: java.util.List[_]) => list.asScala.map(_.toString).toSeq
          case other                         => throw new IllegalArgumentException(s"taxa must be a list, got: $other")
        }
      )
    }
    require(parsed.nonEmpty, "OBO ontology list is empty")
    val prefixes = parsed.map(_.prefix.toLowerCase(java.util.Locale.ROOT))
    require(prefixes.distinct.size == prefixes.size, "duplicate OBO prefixes")
    parsed
  }

  private def str(m: scala.collection.mutable.Map[String, Object], key: String, path: os.Path): String = {
    val v = m.get(key).map(_.toString)
    require(v.exists(_.nonEmpty), s"OBO source entry missing $key in $path")
    v.get
  }

  /** Sources for a downloaded directory: one file per prefix (`mp.obo`, `zp.obo.gz`, ...). */
  def sources(dir: os.Path, entries: Seq[Entry]): Seq[OboSource] = {
    val files = os
      .list(dir)
      .filter(f => os.isFile(f) && (f.last.endsWith(".obo") || f.last.endsWith(".obo.gz")))
      .groupBy(_.last.stripSuffix(".gz").stripSuffix(".obo").toLowerCase(java.util.Locale.ROOT))
    require(files.values.forall(_.size == 1), "ambiguous OBO files: both compressed and uncompressed versions exist")
    val byPrefix = files.map { case (p, paths) => p -> paths.head }
    entries.map { e =>
      val file = byPrefix.getOrElse(
        e.prefix.toLowerCase(java.util.Locale.ROOT),
        throw new IllegalArgumentException(s"no OBO file for prefix ${e.prefix} in $dir")
      )
      new OboSource(file, e.prefix, e.category, e.taxa)
    }
  }
}
