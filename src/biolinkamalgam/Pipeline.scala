package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.export.{DatasetCard, Manifest, Parquet, Tsv}
import biolinkamalgam.source.Registry
import biolinkamalgam.transform.{CliqueMerge, Filters, Uniqueness}
import java.util.concurrent.{Executors, TimeUnit}
import org.apache.spark.sql.SparkSession

/** Stage orchestration. Stages are data-dependent and run sequentially on purpose (ingest → merge → arbitrate); only
  * the four independent SINKS fan out concurrently off one persisted DataFrame — one computation, parallel writes.
  */
object Pipeline {

  case class Options(
      babelDir: Option[os.Path],
      customDir: Option[os.Path],
      outDir: os.Path,
      taxaConfig: os.Path,
      promoteConfig: os.Path,
      filtersConfig: os.Path,
      simpleExport: Boolean = false,
      datasetMeta: Map[String, String] = Map.empty
  )

  def run(spark: SparkSession, opts: Options): Seq[Manifest.Stage] = {
    val stages = scala.collection.mutable.ListBuffer.empty[Manifest.Stage]

    def timed[A](name: String)(body: => (A, Long)): A = {
      val start = System.nanoTime()
      val (result, rows) = body
      stages += Manifest.Stage(name, rows, (System.nanoTime() - start) / 1000000)
      result
    }

    val taxa = Config.loadTaxa(opts.taxaConfig)
    val promotePrefixes = Config.loadPromotePrefixes(opts.promoteConfig)
    val filters = Config.loadFilters(opts.filtersConfig)

    // [1] INGEST — union all registered sources into the canonical clique table,
    // then apply declarative category/prefix/taxon-allowlist filters.
    val cliques = timed("ingest") {
      val sources = Registry.forConfig(opts.babelDir, opts.customDir, promotePrefixes)
      val df = Filters(Registry.loadAll(spark, sources), filters, taxa.allowlist)
      (df, df.count())
    }

    // [2] MERGE CLIQUES — preferred-name string matching, scoped by (category, taxon).
    val (merged, merges) = timed("merge") {
      val result = CliqueMerge(cliques, taxa.majorTaxa)
      result.cliques.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      (result.cliques, result.merges) -> result.cliques.count()
    }

    // [3]+[4] NORMALIZE (gates + l1/l2 keys inside) + ARBITRATE — global synonym uniqueness.
    val (output, conflicts) = timed("arbitrate") {
      val result = Uniqueness(merged, taxa.majorTaxa, filters)
      result.output.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      (result.output, result.conflicts) -> result.output.count()
    }

    // [5] EXPORT — concurrent sink writes off the persisted arbitrated DataFrame.
    timed("export") {
      val pool = Executors.newFixedThreadPool(4)
      try {
        val parquetFut =
          pool.submit[Unit](() => Parquet.writeSingleFile(output, opts.outDir / "biolinkamalgam.parquet", opts.simpleExport))
        val conflictsFut = pool.submit[Unit](() => Tsv.writeSingleFile(conflicts, opts.outDir / "conflicts.tsv"))
        val mergesFut = pool.submit[Unit](() => Tsv.writeSingleFile(merges, opts.outDir / "merges.tsv"))
        parquetFut.get()
        conflictsFut.get()
        mergesFut.get()
        DatasetCard.write(opts.outDir, "biolinkamalgam.parquet", stages.toSeq, opts.datasetMeta)
        () -> output.count()
      } finally {
        pool.shutdown()
        pool.awaitTermination(1, TimeUnit.MINUTES)
      }
    }

    stages.toSeq
  }
}
