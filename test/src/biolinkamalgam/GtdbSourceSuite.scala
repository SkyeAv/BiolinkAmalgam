package biolinkamalgam

import biolinkamalgam.source.{GtdbSource, Registry}
import org.apache.spark.sql.DataFrame
import utest._

/** GTDB metadata ingest (BABEL PR #978 port): rank cliques, NCBITaxon species crossrefs, the many-to-one guard, and
  * by-name column reading over a reduced-column fixture.
  */
object GtdbSourceSuite extends TestSuite {

  private val spark = TestSupport.spark
  private lazy val gtdbDir: os.Path = TestSupport.gtdbDir

  private lazy val gtdb = new GtdbSource().load(spark, gtdbDir)

  private def rowsWhere(df: DataFrame, cond: String): Seq[Map[String, Any]] =
    df.filter(cond).collect().map(r => df.columns.map(c => c -> r.getAs[Any](c)).toMap).toSeq

  val tests = Tests {
    test("higher ranks become deduplicated GTDB taxon cliques with display names") {
      val phylum = rowsWhere(gtdb, "leader = 'GTDB:p__Pseudomonadota'")
      assert(phylum.length == 1)
      assert(phylum.head("preferred_name") == "Pseudomonadota")
      assert(phylum.head("category") == "biolink:OrganismTaxon")
      assert(phylum.head("source") == "gtdb")

      // 6 Escherichia-genome rows share ranks: every higher-rank clique appears exactly once
      val genus = rowsWhere(gtdb, "leader = 'GTDB:g__Escherichia'")
      assert(genus.length == 1)
      assert(genus.head("names") == Seq("Escherichia"))
      assert(genus.head("identifiers") == Seq("GTDB:g__Escherichia"))
      assert(genus.head("taxa") == Seq.empty[String])
    }

    test("species crossref into their NCBITaxon clique (GTDB CURIE as identifier)") {
      val ncbi = rowsWhere(gtdb, "leader = 'NCBITaxon:562'")
      assert(ncbi.length == 1)
      assert(ncbi.head("preferred_name") == "Escherichia coli")
      assert(ncbi.head("names") == Seq("Escherichia coli"))
      assert(ncbi.head("identifiers") == Seq("GTDB:s__Escherichia_coli"))

      val subtilis = rowsWhere(gtdb, "leader = 'NCBITaxon:1423'")
      assert(subtilis.head("identifiers") == Seq("GTDB:s__Bacillus_subtilis"))
    }

    test("many-to-one guard: the alphabetically-first species crossrefs, the rest stay singletons") {
      // GTDB:s__Escherichia_coli and GTDB:s__Escherichia_coli_B both carry ncbi_species_taxid 562.
      // Both must NOT land in NCBITaxon:562; the loser keeps its own GTDB clique.
      assert(rowsWhere(gtdb, "leader = 'NCBITaxon:562'").head("identifiers") == Seq("GTDB:s__Escherichia_coli"))
      val loser = rowsWhere(gtdb, "leader = 'GTDB:s__Escherichia_coli_B'")
      assert(loser.length == 1)
      assert(loser.head("preferred_name") == "Escherichia coli B")
    }

    test("species without a valid taxid stay GTDB singletons") {
      val c = rowsWhere(gtdb, "leader = 'GTDB:s__Escherichia_coli_C'")
      assert(c.length == 1)
      assert(c.head("preferred_name") == "Escherichia coli C")
      assert(c.head("identifiers") == Seq("GTDB:s__Escherichia_coli_C"))
    }

    test("crossref rows fold into BABEL taxon cliques through the Registry leader union") {
      val cliques = Registry.loadAll(
        spark,
        Registry.forConfig(None, None, Some(gtdbDir), Set.empty),
        None
      )
      val ecoli = cliques.filter("leader = 'NCBITaxon:562'").collect()
      assert(ecoli.length == 1)
      assert(ecoli.head.getAs[Int]("identifier_count") == 1)
    }
  }
}
