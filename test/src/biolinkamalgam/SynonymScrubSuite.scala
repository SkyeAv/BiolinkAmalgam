package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.transform.SynonymScrub
import org.apache.spark.sql.DataFrame
import utest._

/** SynonymScrub signals: cohesion outliers, hub strings, cross-clique affinity — and the guardrails that keep legit
  * names (short codes, variants, gene/chemical duals) unflagged. The scrub decides what gets DROPPED from the
  * published dataset, so every signal here has both a positive (flagged) and negative (kept) case.
  */
object SynonymScrubSuite extends TestSuite {

  private val spark = TestSupport.spark
  import spark.implicits._

  private val cfg = Config.ScrubConfig(
    minCliqueMembers = 4,
    cohesionFloor = 0.22,
    hubCliques = 8,
    crossEnabled = true,
    crossTokenDfCap = 2000,
    crossSimilarity = 0.72,
    crossMargin = 0.2,
    minNameLen = 4,
    sampleExamples = 200
  )

  private def cliques(rows: Seq[(String, String, String, Seq[String])]): DataFrame =
    rows.toDF("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS")

  private lazy val flagged: DataFrame = SynonymScrub.flags(testData, cfg)

  private lazy val testData: DataFrame = cliques(Seq(
    // gene clique with one UMLS-style junk label (cohesion outlier)
    ("NCBIGene:1", "Test kinase 1", "biolink:Gene",
      Seq("Test kinase 1", "TEST1", "test kinase one", "kinase, test, 1", "Transthoracic echocardiography")),
    // small clique: cohesion skipped (below min_clique_members)
    ("NCBIGene:2", "Test kinase 2", "biolink:Gene", Seq("Test kinase 2", "zz unrelated word")),
    // hub string across 8 gene cliques
    ("NCBIGene:10", "Kinase A", "biolink:Gene", Seq("Kinase A", "protein")),
    ("NCBIGene:11", "Kinase B", "biolink:Gene", Seq("Kinase B", "protein")),
    ("NCBIGene:12", "Kinase C", "biolink:Gene", Seq("Kinase C", "protein")),
    ("NCBIGene:13", "Kinase D", "biolink:Gene", Seq("Kinase D", "protein")),
    ("NCBIGene:14", "Kinase E", "biolink:Gene", Seq("Kinase E", "protein")),
    ("NCBIGene:15", "Kinase F", "biolink:Gene", Seq("Kinase F", "protein")),
    ("NCBIGene:16", "Kinase G", "biolink:Gene", Seq("Kinase G", "protein")),
    ("NCBIGene:17", "Kinase H", "biolink:Gene", Seq("Kinase H", "protein")),
    // cross: synonym in the WRONG clique — far from every Brk member, near Brca's preferred
    ("NCBIGene:20", "Brk kinase", "biolink:Gene",
      Seq("Brk kinase", "BRK1", "brk one", "Brca1 tumor suppressor")),
    ("NCBIGene:21", "Brca1 tumor suppressor", "biolink:Gene",
      Seq("Brca1 tumor suppressor", "BRCA1", "brca one")),
    // gene/chemical dual recurrence is LEGAL (different classes, never hub-flagged)
    ("CHEBI:555", "Glucose", "biolink:SmallMolecule", Seq("Glucose", "D-glucose", "dextrose")),
    ("NCBIGene:30", "Glucose transporter-like 1", "biolink:Gene", Seq("Glucose transporter-like 1", "GLUT1"))
  ))

  private def flagsFor(signal: String): Seq[(String, String)] =
    flagged.filter($"signal" === signal).select("CURIE", "synonym").collect()
      .map(r => (r.getString(0), r.getString(1))).toSeq

  val tests = Tests {
    test("cohesion: junk label far from its clique is flagged; variants and small cliques stay") {
      val f = flagsFor("cohesion")
      assert(f.contains(("NCBIGene:1", "Transthoracic echocardiography")))
      assert(!f.exists(_._1 == "NCBIGene:2")) // small clique: nothing to compare against
      assert(!f.contains(("NCBIGene:1", "kinase, test, 1"))) // reworded variant of the same name
    }

    test("hub: generic string on many same-class cliques is flagged on each; unique names stay") {
      val f = flagsFor("hub")
      assert(f.count(_._2 == "protein") == 8)
      assert(!f.exists(_._2 == "Kinase A"))
    }

    test("cross: synonym near a foreign preferred name and far from its own clique is flagged") {
      val f = flagsFor("cross")
      assert(f.contains(("NCBIGene:20", "Brca1 tumor suppressor")))
      // a clique's own preferred name is the identity anchor: never cross-flagged, even though
      // "Test kinase 2" is 0.8-similar to sibling clique "Test kinase 1"'s preferred name
      assert(!f.contains(("NCBIGene:2", "Test kinase 2")))
    }

    test("gene/chemical dual recurrence across classes is never flagged") {
      val all = flagged.select("CURIE", "synonym").collect().map(r => (r.getString(0), r.getString(1))).toSeq
      assert(!all.contains(("CHEBI:555", "Glucose")))
      assert(!all.contains(("NCBIGene:30", "Glucose transporter-like 1")))
    }

    test("scrubbed output drops exactly the flagged (CURIE, synonym) pairs") {
      val scrubbed = SynonymScrub.scrubbed(testData, flagged)
      assert(scrubbed.count() == testData.count()) // rows survive; only synonyms drop
      val gene1 = scrubbed.filter($"CURIE" === "NCBIGene:1").select("SYNONYMS").collect().head.getSeq[String](0)
      assert(!gene1.contains("Transthoracic echocardiography"))
      assert(gene1.contains("Test kinase 1"))
    }
  }
}
