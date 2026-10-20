package biolinkamalgam.transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.tartarus.snowball.ext.EnglishStemmer

/** Match-key normalization, mirroring Tablassert's fullmap NLP exactly
  * (rust/src/nlp.rs `normalize_l1`, rust/src/fullmap.rs `clean_slice`/`level_two`):
  *
  *   - `strip`: trim + lowercase + collapse whitespace + drop non-word chars. Used for preferred-name merge matching
  *     only — preferred names must match as written, never stemmed (BiolinkAmalgam's own rule, NOT a fullmap key).
  *   - `clean`: fullmap's `clean_and_lower` — trim + strip matching/duplicate quotes to a fixpoint, then lowercase.
  *   - `l1`: fullmap level-one — clean, split on whitespace, Porter2 (Snowball English) stem every all-ASCII-alphabetic
  *     token (tokens bearing digits/punctuation/non-ASCII pass through verbatim), dedupe, sort, join with single
  *     spaces. Token order and duplicates fold: "oral aspirin" == "aspirin aspirin ORAL".
  *   - `l2`: fullmap level-two — clean, then keep only [a-z0-9_] (spaces and punctuation removed entirely).
  *
  * Output SYNONYMS always keep the ORIGINAL strings; l1/l2 are internal arbitration keys. `l1` exists as a UDF
  * (ThreadLocal stemmer, one per executor thread); `l2` is pure SQL, no UDF cost.
  */
object NameKey {

  private val NonWord = "\\W+".r
  private val Whitespace = "\\s+".r

  def strip(s: String): String =
    if (s == null) ""
    else Whitespace.replaceAllIn(NonWord.replaceAllIn(s.trim.toLowerCase, " "), " ").trim

  /** Spark expression equivalent of `strip` — pure SQL, no UDF cost. */
  def stripKey(c: Column): Column =
    trim(regexp_replace(regexp_replace(lower(trim(c)), "\\W+", " "), "\\s+", " "))

  /** fullmap `clean_and_lower`: trim + strip matching/duplicate quotes to a fixpoint (duplicate leading quote drops
    * ONE char and wins over the matching-quotes strip, exactly as `clean_slice` orders it), then lowercase.
    * Locale.ROOT keeps lowercasing reproducible across executor locales.
    */
  def clean(s: String): String = {
    if (s == null) return ""
    var cur = s
    var done = false
    while (!done) {
      val t = cur.trim
      val duplicateStart =
        t.length >= 2 && ((t.charAt(0) == '\'' && t.charAt(1) == '\'') || (t.charAt(0) == '"' && t.charAt(1) == '"'))
      val matchingQuotes =
        t.length >= 2 && ((t.charAt(0) == '\'' && t.charAt(t.length - 1) == '\'') ||
          (t.charAt(0) == '"' && t.charAt(t.length - 1) == '"'))
      val next =
        if (duplicateStart) t.substring(1)
        else if (matchingQuotes) t.substring(1, t.length - 1)
        else t
      if (next == cur) done = true
      cur = next
    }
    cur.toLowerCase(java.util.Locale.ROOT)
  }

  private val stemmer = new ThreadLocal[EnglishStemmer] {
    override def initialValue(): EnglishStemmer = new EnglishStemmer()
  }

  private def isAsciiAlpha(token: String): Boolean =
    token.forall(c => (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))

  /** fullmap level-one key. Porter2 stems only all-ASCII-alphabetic tokens; identifiers like "tnf-alpha" or "bcl2"
    * survive verbatim. String ordering is code-point order, which matches fullmap's byte-wise sort.
    */
  def l1(s: String): String = {
    val cleaned = clean(s)
    if (cleaned.isEmpty) ""
    else {
      val st = stemmer.get()
      cleaned
        .split("\\s+")
        .map { token =>
          if (isAsciiAlpha(token)) {
            st.setCurrent(token)
            st.stem()
            st.getCurrent
          } else token
        }
        .toSeq
        .distinct
        .sorted
        .mkString(" ")
    }
  }

  /** Level-one key as a UDF. The ThreadLocal stemmer makes this ~as fast as mapPartitions batching (one stemmer per
    * executor thread, no per-row allocation of the stemmer).
    */
  def l1Key(c: Column): Column = udf((s: String) => l1(s)).apply(c)

  /** fullmap level-two key: clean, then keep [a-z0-9_] only. */
  def l2(s: String): String =
    clean(s).filter(c => (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')

  /** Level-two key as pure SQL: quotes and punctuation are not in [a-z0-9_], so the regexp subsumes clean's
    * quote-stripping and no UDF is needed.
    */
  def l2Key(c: Column): Column =
    regexp_replace(lower(trim(c)), "[^a-z0-9_]", "")
}
