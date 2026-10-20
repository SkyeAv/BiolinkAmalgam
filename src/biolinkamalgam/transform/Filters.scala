package biolinkamalgam.transform

import biolinkamalgam.config.Config.FilterConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Ingest-time clique filters, all declarative (config/filters.yaml + the taxa config). Pure DataFrame transforms.
  *
  *   - Category drops: `drop_categories` removes whole categories before merging (biolink:Publication — messy; and
  *     biolink:Agent — BABEL carries it only as leftover-UMLS organization cliques, with no institute-only prefixes to
  *     target and no person cliques to preserve).
  *   - Prefix drops: `drop_curie_prefixes_by_category` removes matching identifiers from cliques in that category; a
  *     clique left with zero identifiers is dropped entirely. This is how institutes are filtered when a source DOES
  *     carry org registries (ROR:, GRID:, ...).
  *   - CURIE regex drops: `exclude_curie_regex` drops cliques whose leader matches (fullmap `exclude_regex` parity).
  *   - Taxon allowlist: fullmap `retain_for_taxon_allowlist` parity — when the allowlist is non-empty, a clique whose
  *     valid taxa (positive NCBITaxon ids; NCBITaxon:0/malformed are not valid) are ALL outside the list is dropped.
  *     Taxonless cliques and biolink:OrganismTaxon cliques are always kept.
  */
object Filters {

  private val ValidTaxon = "^NCBITaxon:[1-9][0-9]*$".r

  /** Pure allowlist retention test (unit-testable, no Spark). */
  def retainForTaxonAllowlist(category: String, taxa: Seq[String], allowlist: Set[String]): Boolean = {
    if (allowlist.isEmpty) return true
    if (category == "biolink:OrganismTaxon") return true
    val valid = Option(taxa).getOrElse(Seq.empty).filter(t => ValidTaxon.pattern.matcher(t).matches())
    valid.isEmpty || valid.exists(allowlist.contains)
  }

  def apply(cliques: DataFrame, filters: FilterConfig, taxonAllowlist: Set[String]): DataFrame = {
    var df = cliques

    if (filters.dropCategories.nonEmpty)
      df = df.filter(!col("category").isin(filters.dropCategories.toSeq: _*))

    if (filters.excludeCurieRegex.nonEmpty) {
      val combined = filters.excludeCurieRegex.map(p => s"($p)").mkString("|")
      df = df.filter(!col("leader").rlike(combined))
    }

    if (filters.dropCuriePrefixesByCategory.nonEmpty) {
      val drops = filters.dropCuriePrefixesByCategory
      val filterIds = udf { (category: String, identifiers: Seq[String]) =>
        drops.get(category) match {
          case None => Option(identifiers).getOrElse(Seq.empty)
          case Some(prefixes) =>
            Option(identifiers).getOrElse(Seq.empty).filterNot(id => prefixes.exists(id.startsWith))
        }
      }
      df = df
        .withColumn("identifiers", filterIds(col("category"), col("identifiers")))
        .filter(size(col("identifiers")) > 0)
        .withColumn("identifier_count", size(col("identifiers")))
    }

    if (taxonAllowlist.nonEmpty) {
      val allow = taxonAllowlist
      val retain = udf { (category: String, taxa: Seq[String]) =>
        retainForTaxonAllowlist(category, Option(taxa).getOrElse(Seq.empty), allow)
      }
      df = df.filter(retain(col("category"), col("taxa")))
    }

    df
  }
}
