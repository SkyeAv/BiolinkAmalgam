package biolinkamalgam.transform

import biolinkamalgam.config.Config.ScrubConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Report-only lexical diagnostics. Neither dissimilarity nor cross-taxon recurrence proves a wrong synonym. Work is
  * bounded: score only eligible cliques, then join outliers to a capped exact preferred-key index. Never build a global
  * fuzzy-similarity graph. Original strings and identifier aliases remain untouched.
  */
object SynonymScrub {
  private val Identifier = "^[A-Za-z][A-Za-z0-9_.-]*:[^\\s]+$".r

  def trigrams(s: String): Seq[String] = {
    val t = NameKey.strip(s).replace(" ", "")
    if (t.length < 3) Seq.empty else t.sliding(3).toSeq.distinct
  }

  def jaccard(a: Seq[String], b: Seq[String]): Double = similarity(a.toSet, b.toSet)

  private def similarity(a: Set[String], b: Set[String]): Double = {
    val intersection = a.count(b.contains)
    val union = a.size + b.size - intersection
    if (union == 0) 0.0 else intersection.toDouble / union
  }

  private def protectedName(name: String, preferred: String): Boolean =
    name == null || Identifier.pattern.matcher(name).matches() ||
      NameKey.strip(name) == NameKey.strip(preferred)

  /** Precompute sets once per clique; short codes and identifiers are not outlier candidates. */
  private def scores(names: Seq[String], preferred: String, minLength: Int): Seq[(String, Double)] = {
    val members = Option(names)
      .getOrElse(Seq.empty)
      .filter(_ != null)
      .distinct
      .map(n => n -> trigrams(n).toSet)
    members.collect {
      case (name, grams) if name.length >= minLength && grams.nonEmpty && !protectedName(name, preferred) =>
        val best = members.iterator
          .collect {
            case (other, otherGrams) if other != name => similarity(grams, otherGrams)
          }
          .foldLeft(0.0)(math.max)
        name -> best
    }
  }

  /** Bounds apply BEFORE the UDF. Cliques outside the bounds are skipped, not declared clean. */
  def cohesionTable(df: DataFrame, cfg: ScrubConfig): DataFrame = {
    val score = udf((names: Seq[String], preferred: String) => scores(names, preferred, cfg.minNameLen))
    df.filter(size(col("SYNONYMS")) >= cfg.minCliqueMembers && size(col("SYNONYMS")) <= cfg.maxCliqueMembers)
      .select(col("CURIE"), col("CLASS"), explode(score(col("SYNONYMS"), col("PREFERRED_NAME"))).as("e"))
      .select(col("CURIE"), col("CLASS"), col("e._1").as("synonym"), col("e._2").as("own_cohesion"))
  }

  /** CLASS, CURIE, synonym, signal, detail. A pair may carry multiple independent signals. */
  def flags(df: DataFrame, cfg: ScrubConfig): DataFrame = flags(df, cfg, cohesionTable(df, cfg))

  /** The CLI owns persistence of scored rows so every branch reuses one bounded scoring pass. */
  def flags(df: DataFrame, cfg: ScrubConfig, scored: DataFrame): DataFrame = {
    val eligible = udf((name: String, preferred: String) => !protectedName(name, preferred))
    val outliers = scored.filter(col("own_cohesion") < cfg.cohesionFloor)
    val cohesion = outliers.select(
      col("CLASS"),
      col("CURIE"),
      col("synonym"),
      lit("cohesion").as("signal"),
      concat(lit("review only; own_cohesion="), col("own_cohesion")).as("detail")
    )

    val names = df
      .select(col("CURIE"), col("CLASS"), col("PREFERRED_NAME"), explode(col("SYNONYMS")).as("synonym"))
      .filter(eligible(col("synonym"), col("PREFERRED_NAME")))
    val hubs = names
      .groupBy("CLASS", "synonym")
      .agg(count_distinct(col("CURIE")).as("cliques"))
      .filter(col("cliques") >= cfg.hubCliques)
    val hub = names
      .join(hubs, Seq("CLASS", "synonym"))
      .select(
        col("CLASS"),
        col("CURIE"),
        col("synonym"),
        lit("hub").as("signal"),
        concat(lit("review only; cross-taxon recurrence may be legal; cliques="), col("cliques")).as("detail")
      )

    if (!cfg.crossEnabled) cohesion.unionByName(hub)
    else {
      // Aggregate the target bucket BEFORE joining aliases. Each outlier has at most crossKeyMaxTargets matches.
      val preferred = df
        .select(
          col("CLASS"),
          NameKey.stripKey(col("PREFERRED_NAME")).as("key"),
          col("CURIE").as("target"),
          col("PREFERRED_NAME").as("target_name")
        )
        .filter(length(col("key")) > 0)
        .join(
          outliers.select(col("CLASS"), NameKey.stripKey(col("synonym")).as("key")).distinct(),
          Seq("CLASS", "key"),
          "left_semi"
        )
      val smallKeys = preferred
        .groupBy("CLASS", "key")
        .count()
        .filter(col("count") <= cfg.crossKeyMaxTargets)
        .drop("count")
      val index = preferred.join(smallKeys, Seq("CLASS", "key"))
      val sim = udf((a: String, b: String) => jaccard(trigrams(a), trigrams(b)))
      val cross = outliers
        .withColumn("key", NameKey.stripKey(col("synonym")))
        .join(index, Seq("CLASS", "key"))
        .filter(col("CURIE") =!= col("target"))
        .withColumn("sim", sim(col("synonym"), col("target_name")))
        .filter(col("sim") >= cfg.crossSimilarity && col("sim") - col("own_cohesion") >= cfg.crossMargin)
        .groupBy("CLASS", "CURIE", "synonym")
        .agg(max(struct(col("sim"), col("target"))).as("best"))
        .select(
          col("CLASS"),
          col("CURIE"),
          col("synonym"),
          lit("cross").as("signal"),
          concat(lit("review only; exact preferred-key match="), col("best.target"), lit("; sim="), col("best.sim"))
            .as("detail")
        )
      cohesion.unionByName(hub).unionByName(cross)
    }
  }
}
