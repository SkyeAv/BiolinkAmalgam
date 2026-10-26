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

  /** Declarative synonym-quality thresholds (config/synonym_scrub.yaml). Every number here decides which synonyms get
    * dropped as "probably incorrect", so the yaml carries a why-comment per entry and the scrub tool runs in a
    * report-only mode first: inspect the evidence BEFORE trusting the thresholds on real data.
    */
  case class ScrubConfig(
      minCliqueMembers: Int,
      cohesionFloor: Double,
      hubCliques: Int,
      crossEnabled: Boolean,
      crossKeyMaxTargets: Int,
      maxCliqueMembers: Int,
      crossSimilarity: Double,
      crossMargin: Double,
      minNameLen: Int,
      sampleExamples: Int
  ) extends Serializable {
    require(minCliqueMembers >= 2, "min_clique_members must be >= 2")
    require(maxCliqueMembers >= minCliqueMembers, "max_clique_members must be >= min_clique_members")
    require(hubCliques >= 2 && crossKeyMaxTargets >= 1, "hub_cliques must be >= 2; cross_key_max_targets must be >= 1")
    require(minNameLen >= 3 && sampleExamples >= 1, "min_name_len must be >= 3; sample_examples must be >= 1")
    require(
      Seq(cohesionFloor, crossSimilarity, crossMargin).forall(v => v.isFinite && v >= 0 && v <= 1),
      "scrub similarity thresholds must be finite and between 0 and 1"
    )
  }

  object ScrubConfig {
    val empty: ScrubConfig = ScrubConfig(
      minCliqueMembers = 4,
      cohesionFloor = 0.22,
      hubCliques = 8,
      crossEnabled = true,
      crossKeyMaxTargets = 4,
      maxCliqueMembers = 64,
      crossSimilarity = 0.72,
      crossMargin = 0.2,
      minNameLen = 4,
      sampleExamples = 200
    )
  }

  /** aria2c tuning (config/download.yaml `aria2:`). */
  case class Aria2Config(
      binary: String = "aria2c",
      connectionsPerFile: Int = 16,
      split: Int = 16,
      minSplitSize: String = "16M",
      concurrentFiles: Int = 8,
      extraArgs: Seq[String] = Seq.empty
  )

  /** Snapshot download settings (config/download.yaml). `engine` is `aria2` or `jdk`. */
  case class DownloadConfig(
      engine: String = "aria2",
      baseUrl: String = "https://stars.renci.org/var/babel_outputs",
      aria2: Aria2Config = Aria2Config(),
      jdkConcurrency: Int = 8,
      chunkedConnectionsPerFile: Int = 8,
      excludeFileRegex: Seq[String] = Seq.empty
  )

  def loadDownload(path: os.Path): DownloadConfig = {
    if (!os.exists(path)) return DownloadConfig()
    val doc = yamlDoc(path)
    val d = DownloadConfig()
    val a = Option(doc.get("aria2")).map(_.asInstanceOf[java.util.Map[String, AnyRef]]).getOrElse(new java.util.HashMap)
    def str(m: java.util.Map[String, AnyRef], k: String, dflt: String) =
      Option(m.get(k)).map(_.toString).getOrElse(dflt)
    def int(m: java.util.Map[String, AnyRef], k: String, dflt: Int) =
      Option(m.get(k)).map(_.toString.toInt).getOrElse(dflt)
    val engine = str(doc, "engine", d.engine)
    if (engine != "aria2" && engine != "jdk" && engine != "jdk-chunked")
      throw new IllegalArgumentException(
        s"download engine must be 'aria2', 'jdk', or 'jdk-chunked', got '$engine' in $path"
      )
    DownloadConfig(
      engine = engine,
      baseUrl = str(doc, "base_url", d.baseUrl),
      aria2 = Aria2Config(
        binary = str(a, "binary", d.aria2.binary),
        connectionsPerFile = int(a, "connections_per_file", d.aria2.connectionsPerFile),
        split = int(a, "split", d.aria2.split),
        minSplitSize = str(a, "min_split_size", d.aria2.minSplitSize),
        concurrentFiles = int(a, "concurrent_files", d.aria2.concurrentFiles),
        extraArgs = Option(a.get("extra_args"))
          .map(_.asInstanceOf[java.util.List[String]].asScala.toSeq)
          .getOrElse(Seq.empty)
      ),
      jdkConcurrency = int(doc, "jdk_concurrency", d.jdkConcurrency),
      chunkedConnectionsPerFile = int(doc, "chunked_connections_per_file", d.chunkedConnectionsPerFile),
      excludeFileRegex = stringList(doc, "exclude_file_regex")
    )
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

  /** Synonym-quality thresholds. Missing files and unknown keys fail rather than silently changing analysis. */
  def loadScrub(path: os.Path): ScrubConfig = {
    val d = ScrubConfig.empty
    require(os.exists(path), s"scrub config does not exist: $path")
    val doc = yamlDoc(path)
    require(doc != null, s"empty scrub config: $path")
    val allowed = Set(
      "min_clique_members",
      "max_clique_members",
      "cohesion_floor",
      "hub_cliques",
      "cross_enabled",
      "cross_key_max_targets",
      "cross_similarity",
      "cross_margin",
      "min_name_len",
      "sample_examples"
    )
    require(doc.keySet().asScala.forall(allowed.contains), s"unknown scrub config keys in $path")
    def intOpt(key: String, default: Int): Int = Option(doc.get(key)).map(_.toString.toInt).getOrElse(default)
    def dblOpt(key: String, default: Double): Double = Option(doc.get(key)).map(_.toString.toDouble).getOrElse(default)
    def boolOpt(key: String, default: Boolean): Boolean =
      Option(doc.get(key)).map(_.toString.toBoolean).getOrElse(default)
    d.copy(
      minCliqueMembers = intOpt("min_clique_members", d.minCliqueMembers),
      cohesionFloor = dblOpt("cohesion_floor", d.cohesionFloor),
      hubCliques = intOpt("hub_cliques", d.hubCliques),
      crossEnabled = boolOpt("cross_enabled", d.crossEnabled),
      crossKeyMaxTargets = intOpt("cross_key_max_targets", d.crossKeyMaxTargets),
      maxCliqueMembers = intOpt("max_clique_members", d.maxCliqueMembers),
      crossSimilarity = dblOpt("cross_similarity", d.crossSimilarity),
      crossMargin = dblOpt("cross_margin", d.crossMargin),
      minNameLen = intOpt("min_name_len", d.minNameLen),
      sampleExamples = intOpt("sample_examples", d.sampleExamples)
    )
  }

  /** Map a taxon to its scope: itself if major, OtherTaxa otherwise. */
  def taxonScope(taxon: String, majorTaxa: Set[String]): String =
    if (majorTaxa.contains(taxon)) taxon else OtherTaxa

  /** All scopes for a clique's taxa list (empty list shares the no-taxon bucket). */
  def taxonScopes(taxa: Seq[String], majorTaxa: Set[String]): Seq[String] =
    if (taxa.isEmpty) Seq(NoTaxon) else taxa.map(taxonScope(_, majorTaxa)).distinct
}
