package biolinkamalgam.transform

import biolinkamalgam.config.Config.FilterConfig

import scala.util.matching.Regex

/** Junk admission gates (NOT normalization — the dataset ships original strings and downstream consumers do the real
  * NLP). Gates run on the cleaned (quote-stripped, lowercased, punctuation-intact) form so the numeric regex still sees
  * the `.` in `0.5`. Everything data-driven lives in `FilterConfig` (config/filters.yaml); the code owns no lists.
  */
object TermGates {

  /** Reject empty terms, terms containing tab/CR/LF, and terms containing any banned substring (literal needles on the
    * cleaned lowercase form).
    */
  def tokenQc(cleaned: String, bannedSubstrings: Seq[String]): Boolean =
    cleaned.nonEmpty &&
      !cleaned.contains('\t') &&
      !cleaned.contains('\n') &&
      !cleaned.contains('\r') &&
      !bannedSubstrings.exists(cleaned.contains)

  /** Exact-match stopwords (medically meaningless sentinel/common words) plus the configured junk regexes — the numeric
    * pattern covers integers AND decimals (0.5, 1.05 acquire garbage UMLS CURIEs upstream).
    */
  def isDeadTerm(term: String, deadTerms: Set[String], excludeRegex: Seq[Regex]): Boolean =
    term.isEmpty || deadTerms.contains(term) || excludeRegex.exists(_.pattern.matcher(term).matches())

  /** Clean a raw synonym and admit it if it passes every gate. Returns the cleaned form when admitted, null when
    * rejected — shaped for a single UDF that both gates and feeds the arbitration key.
    */
  def admitted(raw: String, cfg: FilterConfig): String = {
    val cleaned = NameKey.clean(raw)
    if (tokenQc(cleaned, cfg.bannedSubstrings) && !isDeadTerm(cleaned, cfg.deadTerms, cfg.compiledExcludeNameRegex))
      cleaned
    else null
  }
}
