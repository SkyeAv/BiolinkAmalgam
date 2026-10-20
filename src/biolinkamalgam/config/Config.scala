package biolinkamalgam.config

import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._

/** File-backed configuration: major taxa, promoted identifier prefixes, resource profiles.
  *
  * Big, reviewable decisions live in the config yaml files (BABEL's configuration-over-constants convention), not in
  * code. Every entry in those files carries a comment saying why.
  */
object Config {

  /** Declarative run filters (config/filters.yaml): category/prefix drops plus the term-admission gates.
    * `compiledExcludeNameRegex` is lazy so the config stays Serializable for UDF capture.
    */
  case class FilterConfig(
      dropCategories: Set[String],
      dropCuriePrefixesByCategory: Map[String, Set[String]],
      bannedSubstrings: Seq[String],
      deadTerms: Set[String],
      excludeNameRegex: Seq[String],
      excludeCurieRegex: Seq[String]
  ) extends Serializable {
    @transient lazy val compiledExcludeNameRegex: Seq[scala.util.matching.Regex] = excludeNameRegex.map(_.r)
    @transient lazy val compiledExcludeCurieRegex: Seq[scala.util.matching.Regex] = excludeCurieRegex.map(_.r)
  }

  object FilterConfig {
    val empty: FilterConfig = FilterConfig(Set.empty, Map.empty, Seq.empty, Set.empty, Seq.empty, Seq.empty)
  }

  /** Taxon scoping for merge and arbitration. */
  val NoTaxon: String = "__NO_TAXON__"
  val OtherTaxa: String = "__OTHER_TAXA__"

  /** Major-taxon bucketing plus the optional fullmap-parity taxon allowlist. */
  case class TaxaConfig(majorTaxa: Set[String], allowlist: Set[String])

  def loadTaxa(path: os.Path): TaxaConfig = {
    val doc = yamlDoc(path)
    TaxaConfig(
      majorTaxa = stringList(doc, "major_taxa").toSet,
      allowlist = stringList(doc, "taxon_allowlist").toSet
    )
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

  private def stringList(doc: java.util.Map[String, AnyRef], key: String): Seq[String] =
    Option(doc.get(key))
      .map(_.asInstanceOf[java.util.List[String]].asScala.toSeq)
      .getOrElse(Seq.empty)

  /** Declarative filters. Every key is optional; an absent key means "no filtering of that kind". */
  def loadFilters(path: os.Path): FilterConfig = {
    if (!os.exists(path)) return FilterConfig.empty
    val doc = yamlDoc(path)
    val prefixesByCategory = Option(doc.get("drop_curie_prefixes_by_category"))
      .map(
        _.asInstanceOf[java.util.Map[String, java.util.List[String]]].asScala
          .map { case (k, v) => k -> v.asScala.toSet }
          .toMap
      )
      .getOrElse(Map.empty)
    FilterConfig(
      dropCategories = stringList(doc, "drop_categories").toSet,
      dropCuriePrefixesByCategory = prefixesByCategory,
      bannedSubstrings = stringList(doc, "banned_substrings").map(_.toLowerCase(java.util.Locale.ROOT)),
      deadTerms = stringList(doc, "dead_terms").map(_.toLowerCase(java.util.Locale.ROOT)).toSet,
      excludeNameRegex = stringList(doc, "exclude_name_regex"),
      excludeCurieRegex = stringList(doc, "exclude_curie_regex")
    )
  }

  /** Map a taxon to its scope: itself if major, OtherTaxa otherwise. */
  def taxonScope(taxon: String, majorTaxa: Set[String]): String =
    if (majorTaxa.contains(taxon)) taxon else OtherTaxa

  /** All scopes for a clique's taxa list (empty list shares the no-taxon bucket). */
  def taxonScopes(taxa: Seq[String], majorTaxa: Set[String]): Seq[String] =
    if (taxa.isEmpty) Seq(NoTaxon) else taxa.map(taxonScope(_, majorTaxa)).distinct
}
