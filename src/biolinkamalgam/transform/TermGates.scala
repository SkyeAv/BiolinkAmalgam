package biolinkamalgam.transform

import biolinkamalgam.config.Config.FilterConfig
import scala.util.matching.Regex

/** Term admission gates, mirroring Tablassert's fullmap emit path (rust/src/fullmap.rs `token_qc`/`is_dead_term`,
  * `emit_term`): a term is QC-ed on its cleaned (pre-stem) lowercase form, dead-term checked on BOTH the cleaned form
  * and the level-one key it is about to be stored under.
  *
  * Everything data-driven lives in `FilterConfig` (config/filters.yaml) — the code owns no lists.
  */
object TermGates {

  /** fullmap `token_qc` parity: reject empty terms, terms containing tab/CR/LF, and terms containing any banned
    * substring. Banned needles are literal lowercase spellings checked BEFORE stemming (Porter2 would turn
    * "hypothetical" into "hypothet" and the substring check would silently stop matching). `cleaned` must already be
    * lowercase — `NameKey.clean` output.
    */
  def tokenQc(cleaned: String, bannedSubstrings: Seq[String]): Boolean =
    cleaned.nonEmpty &&
      !cleaned.contains('\t') &&
      !cleaned.contains('\n') &&
      !cleaned.contains('\r') &&
      !bannedSubstrings.exists(cleaned.contains)

  /** fullmap `is_dead_term` parity, extended: exact-match stopwords (fullmap's sentinel set plus the configured
    * medically meaningless common words) and the configured numeric/junk regexes (covers integers AND decimals like
    * 0.5 or 1.05, which acquire garbage UMLS CURIEs upstream).
    */
  def isDeadTerm(term: String, deadTerms: Set[String], excludeRegex: Seq[Regex]): Boolean =
    term.isEmpty || deadTerms.contains(term) || excludeRegex.exists(_.pattern.matcher(term).matches())

  /** The full emit-path admission test for a raw synonym string: clean, QC, dead-check the cleaned form, then
    * dead-check the level-one key (the exact string a lookup would probe). Returns true when the term may be stored.
    */
  def admit(raw: String, cfg: FilterConfig): Boolean = {
    val cleaned = NameKey.clean(raw)
    if (!tokenQc(cleaned, cfg.bannedSubstrings)) return false
    val exclude = cfg.compiledExcludeNameRegex
    if (isDeadTerm(cleaned, cfg.deadTerms, exclude)) return false
    !isDeadTerm(NameKey.l1(raw), cfg.deadTerms, exclude)
  }
}
