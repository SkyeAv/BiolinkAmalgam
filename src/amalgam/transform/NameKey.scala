package amalgam.transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.tartarus.snowball.ext.PorterStemmer

/** Match-key normalization. Two functions only:
  *
  *   - `strip`: trim + lowercase + collapse whitespace + drop non-word chars. Used for preferred-name merge matching —
  *     preferred names must match as written, never stemmed.
  *   - `stem`: strip → tokenize → Porter-stem each token → rejoin. Used for synonym arbitration. Output SYNONYMS keep
  *     the ORIGINAL strings; stems are internal keys.
  *
  * Both exist as plain Scala functions (unit-testable, no Spark) and as Spark Columns.
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

  private val stemmer = new ThreadLocal[PorterStemmer] {
    override def initialValue(): PorterStemmer = new PorterStemmer()
  }

  def stem(s: String): String = {
    val stripped = strip(s)
    if (stripped.isEmpty) ""
    else {
      val st = stemmer.get()
      stripped
        .split(" ")
        .map { token =>
          st.setCurrent(token)
          st.stem()
          st.getCurrent
        }
        .mkString(" ")
    }
  }

  /** Stemmed key as a UDF. The ThreadLocal stemmer makes this ~as fast as mapPartitions batching (one stemmer per
    * executor thread, no per-row allocation of the stemmer).
    */
  def stemKey(c: Column): Column = udf((s: String) => stem(s)).apply(c)
}
