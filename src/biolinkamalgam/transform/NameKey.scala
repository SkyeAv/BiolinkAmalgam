package biolinkamalgam.transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/** Match-key normalization. Deliberately minimal — this dataset ships ORIGINAL strings and leaves real NLP (Porter2
  * stemming, level-one/level-two keys) to downstream consumers (Tablassert's fullmap); maintaining two NLP pipelines in
  * two languages is how they drift. What lives here is only what the pipeline itself needs:
  *
  *   - `strip`: trim + lowercase + drop non-word chars + collapse whitespace. The key for preferred-name merge matching
  *     AND synonym arbitration: case/punctuation/whitespace variants are the same name; inflections are NOT merged (no
  *     stemming anywhere in this codebase).
  *   - `clean`: trim + strip matching/duplicate quotes to a fixpoint, then lowercase. The input to the junk gates
  *     (TermGates), which must see punctuation intact (e.g. the numeric-regex gate must still see the `.` in `0.5`).
  *
  * Both exist as plain Scala functions (unit-testable, no Spark) and as Spark expressions (pure SQL, no UDF cost).
  */
object NameKey {

  private val NonWord = "\\W+".r
  private val Whitespace = "\\s+".r

  def strip(s: String): String =
    if (s == null) ""
    else Whitespace.replaceAllIn(NonWord.replaceAllIn(s.trim.toLowerCase, " "), " ").trim

  /** Spark expression equivalent of `strip` — pure SQL. */
  def stripKey(c: Column): Column =
    trim(regexp_replace(regexp_replace(lower(trim(c)), "\\W+", " "), "\\s+", " "))

  /** Trim + strip matching/duplicate quotes to a fixpoint (duplicate leading quote drops ONE char and wins over the
    * matching-quotes strip), then lowercase. Locale.ROOT keeps lowercasing reproducible across executor locales.
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
}
