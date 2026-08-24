package amalgam.config

import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._

/** File-backed configuration: major taxa, promoted identifier prefixes, resource profiles.
  *
  * Big, reviewable decisions live in the config yaml files (BABEL's configuration-over-constants convention), not in
  * code. Every entry in those files carries a comment saying why.
  */
object Config {

  /** Taxon scoping for merge and arbitration. */
  val NoTaxon: String = "__NO_TAXON__"
  val OtherTaxa: String = "__OTHER_TAXA__"

  def loadTaxa(path: os.Path): Set[String] = {
    val doc = yamlDoc(path)
    doc
      .get("major_taxa")
      .asInstanceOf[java.util.List[String]]
      .asScala
      .toSet
  }

  def loadPromotePrefixes(path: os.Path): Set[String] = {
    val doc = yamlDoc(path)
    doc
      .get("promote_prefixes")
      .asInstanceOf[java.util.List[String]]
      .asScala
      .map(_.stripSuffix(":"))
      .toSet
  }

  def loadProfile(path: os.Path, name: String): Map[String, String] = {
    val doc = yamlDoc(path)
    val profiles = doc.get("profiles")
    if (profiles == null) return Map.empty
    val profile = profiles.asInstanceOf[java.util.Map[String, AnyRef]].get(name)
    if (profile == null)
      throw new IllegalArgumentException(s"no profile named '$name' in $path")
    profile.asInstanceOf[java.util.Map[String, String]].asScala.toMap
  }

  /** snakeyaml's `Yaml.load[T]` is generic — without an explicit type parameter Scala infers Nothing and the cast to
    * LinkedHashMap explodes at runtime.
    */
  private def yamlDoc(path: os.Path): java.util.Map[String, AnyRef] = {
    val in = java.nio.file.Files.newInputStream(path.toNIO)
    try new Yaml().load[java.util.Map[String, AnyRef]](in)
    finally in.close()
  }

  /** Map a taxon to its scope: itself if major, OtherTaxa otherwise. */
  def taxonScope(taxon: String, majorTaxa: Set[String]): String =
    if (majorTaxa.contains(taxon)) taxon else OtherTaxa

  /** All scopes for a clique's taxa list (empty list shares the no-taxon bucket). */
  def taxonScopes(taxa: Seq[String], majorTaxa: Set[String]): Seq[String] =
    if (taxa.isEmpty) Seq(NoTaxon) else taxa.map(taxonScope(_, majorTaxa)).distinct
}
