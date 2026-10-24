package biolinkamalgam.transform

import biolinkamalgam.config.Config.ScrubConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

/** Post-arbitration synonym-quality filter: drops synonyms that LOOK wrong by structural evidence, never by stemming.
  *
  * Signals (all thresholds in config/synonym_scrub.yaml, report-only mode first):
  *
  *   - `cohesion` — a synonym whose best character-trigram Jaccard against its own clique's OTHER members is below
  *     `cohesion_floor` (cliques with >= `min_clique_members` members). The classic UMLS failure: an unrelated label
  *     glued into a gene/chemical clique scores ~0 against every real member.
  *   - `hub` — the same string attached to >= `hub_cliques` distinct cliques within one class. Generic vocabulary
  *     pollution, not a name. Cross-class recurrence stays legal (gene/chemical duals are real).
  *   - `cross` — a synonym whose trigram similarity to a FOREIGN clique's preferred name (same class, different
  *     CURIE) clears `cross_similarity` AND beats its own-clique cohesion by `cross_margin`: assigned to the wrong
  *     clique. Candidate pairs come from a deterministic MinHash banding LSH over trigrams (24 hashes, 12 bands of 2
  *     — catches Jaccard >= 0.72 pairs with ~1.0 probability; exact Jaccard confirms) — all-pairs is impossible at
  *     139M cliques, and spark-mllib is deliberately not in the fat jar.
  *
  * The result's `report` is the evidence: one row per flagged synonym with signal + numbers. Review it before
  * applying; `scrubbed` is the input minus every flagged (CURIE, synonym).
  */
object SynonymScrub {

  private val NumHashes = 24
  private val BandSize = 2

  /** (synonym, best) pairs per clique: best = max trigram Jaccard against any OTHER member. */
  private val cohesionUdf = udf { synonyms: Seq[String] =>
    val tgs = synonyms.map(n => n -> trigrams(n))
    tgs.map { case (n, tg) =>
      val best = tgs.foldLeft(0.0) { case (acc, (m, og)) =>
        if (m == n || tg.isEmpty || og.isEmpty) acc else math.max(acc, jaccard(tg, og))
      }
      (n, best)
    }
  }

  def trigrams(s: String): Seq[String] = {
    val t = NameKey.strip(s).replaceAll(" ", "")
    if (t.isEmpty || t.length <= 3) Seq.empty
    else t.sliding(3).toSeq.distinct
  }

  def jaccard(a: Seq[String], b: Seq[String]): Double = {
    if (a.isEmpty || b.isEmpty) return 0.0
    val (av, bv) = (a.toSet, b.toSet)
    av.intersect(bv).size.toDouble / av.union(bv).size
  }

  /** One MinHash of the trigram set: the minimum MurmurHash under seed `seed`. Deterministic across runs/JVMs. */
  private val minHashUdf = udf { grams: Seq[String] =>
    if (grams.isEmpty) Array.fill(NumHashes)(Int.MinValue)
    else
      Array.tabulate(NumHashes) { i =>
        grams.iterator.map(g => scala.util.hashing.MurmurHash3.stringHash(g, i * 1000003)).min
      }
  }

  /** Per-synonym own-clique cohesion over ALL cliques (small cliques included; the cross signal needs it). */
  def cohesionTable(df: DataFrame): DataFrame =
    df.select(col("CURIE"), col("CLASS"), size(col("SYNONYMS")).as("members"), cohesionUdf(col("SYNONYMS")).as("scored"))
      .select(col("CURIE"), col("CLASS"), col("members"), explode(col("scored")).as("e"))
      .select(
        col("CURIE").as("c_curie"), col("CLASS").as("c_class"), col("members"),
        col("e._1").as("c_synonym"), col("e._2").as("own_cohesion")
      )

  /** Flagged synonyms: CLASS, CURIE, synonym, signal, detail (human-readable numbers). Union of all signals. */
  def flags(df: DataFrame, cfg: ScrubConfig): DataFrame = {
    val cohesion = cohesionTable(df)
      .filter(col("members") >= cfg.minCliqueMembers)
      .filter(length(col("c_synonym")) >= cfg.minNameLen && col("own_cohesion") < cfg.cohesionFloor)
      .select(col("c_class").as("CLASS"), col("c_curie").as("CURIE"), col("c_synonym").as("synonym"),
        lit("cohesion").as("signal"), concat(lit("own_cohesion="), col("own_cohesion")).as("detail"))

    val exploded = df.select(col("CURIE"), col("CLASS"), explode(col("SYNONYMS")).as("synonym"))
    val hubStrings = exploded
      .groupBy(col("CLASS"), col("synonym"))
      .agg(approx_count_distinct(col("CURIE")).as("cliques"))
      .filter(col("cliques") >= cfg.hubCliques)
    val hub = exploded
      .join(hubStrings, Seq("CLASS", "synonym"))
      .select(col("CLASS"), col("CURIE"), col("synonym"), lit("hub").as("signal"),
        concat(lit("cliques="), col("cliques")).as("detail"))

    val cross = if (cfg.crossEnabled) crossFlags(df, cfg) else emptyFlags(df)

    Seq(cohesion, hub, cross).reduce(_ unionByName _)
  }

  /** MinHash banding LSH: synonyms x same-class preferred-name candidates, exact-Jaccard confirm, foreign CURIE, and
    * the foreign match must beat own cohesion by cross_margin (small cliques: cohesion reads 0, so a confidently
    * foreign name can still fire).
    */
  private def crossFlags(df: DataFrame, cfg: ScrubConfig): DataFrame = {
    val tg = udf((s: String) => trigrams(s).toArray)

    // The own preferred name is the clique's identity anchor: a synonym that IS it (same strip key)
    // is never "assigned to the wrong clique", however similar it is to a sibling clique's name.
    val ownPref = df.select(col("CURIE"), NameKey.stripKey(col("PREFERRED_NAME")).as("opk"))
    val synonyms = df
      .select(col("CURIE"), col("CLASS"), explode(col("SYNONYMS")).as("synonym"))
      .filter(length(col("synonym")) >= cfg.minNameLen)
      .join(ownPref, Seq("CURIE"))
      .filter(NameKey.stripKey(col("synonym")) =!= col("opk"))
      .withColumn("grams", tg(col("synonym")))
      .withColumn("sig", minHashUdf(col("grams")))
    val preferred = df
      .select(col("CURIE").as("pref_curie"), col("CLASS").as("pclass"), col("PREFERRED_NAME").as("pname"))
      .filter(length(col("pname")) >= cfg.minNameLen)
      .withColumn("grams", tg(col("pname")))
      .withColumn("sig", minHashUdf(col("grams")))

    // band id b holds hashes [b*BandSize, (b+1)*BandSize): identical full band => candidate pair
    val synBands = (0 until NumHashes / BandSize).map { b =>
      synonyms.select(col("CURIE").as("syn_curie"), col("CLASS").as("sclass"), col("synonym"), col("grams").as("s_grams"),
        array((0 until BandSize).map(k => col("sig")(b * BandSize + k)): _*).as("band")).withColumn("b", lit(b))
    }.reduce(_ unionByName _)

    val prefBands = (0 until NumHashes / BandSize).map { b =>
      preferred.select(col("pref_curie"), col("pclass"), col("pname"), col("grams"),
        array((0 until BandSize).map(k => col("sig")(b * BandSize + k)): _*).as("band")).withColumn("b", lit(b))
    }.reduce(_ unionByName _)

    val candidates = synBands
      .join(prefBands, Seq("b", "band"))
      .filter(col("sclass") === col("pclass") && col("syn_curie") =!= col("pref_curie"))
      .select(col("sclass"), col("syn_curie"), col("synonym"), col("pref_curie"), col("s_grams"), col("pname"))
      .distinct()

    val gramUdf = udf((a: Seq[String], p: String) => jaccard(a, trigrams(p)))
    val best = candidates
      .withColumn("sim", gramUdf(col("s_grams"), col("pname")))
      .filter(col("sim") >= cfg.crossSimilarity)
      .groupBy(col("sclass"), col("syn_curie"), col("synonym"))
      .agg(max(struct(col("sim"), col("pref_curie"))).as("bestm"))
      .select(col("sclass"), col("syn_curie"), col("synonym"), col("bestm.sim").as("sim"),
        col("bestm.pref_curie").as("detail_curie"))

    best
      .join(cohesionTable(df), col("syn_curie") === col("c_curie") && col("synonym") === col("c_synonym"), "left")
      .filter(col("members").isNull || (col("sim") - col("own_cohesion")) >= cfg.crossMargin)
      .select(
        col("sclass").as("CLASS"), col("syn_curie").as("CURIE"), col("synonym"), lit("cross").as("signal"),
        concat(lit("sim="), col("sim"), lit(" matches "), col("detail_curie")).as("detail"))
  }

  private def emptyFlags(df: DataFrame): DataFrame =
    df.select(col("CLASS"), col("CURIE"), lit("").as("synonym"), lit("").as("signal"), lit("").as("detail")).limit(0)

  /** Input minus every flagged synonym: drop-set per CURIE, array_except on SYNONYMS (order-preserving). */
  def scrubbed(df: DataFrame, flagged: DataFrame): DataFrame = {
    val toDrop = flagged.groupBy("CURIE").agg(collect_set("synonym").as("drop"))
    val emptyArr = array().cast("array<string>")
    df.join(toDrop, Seq("CURIE"), "left")
      .select(
        col("CURIE"), col("PREFERRED_NAME"), col("CLASS"),
        coalesce(array_except(col("SYNONYMS"), coalesce(col("drop"), emptyArr)), emptyArr).as("SYNONYMS")
      )
  }
}
