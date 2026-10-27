package biolinkamalgam

import biolinkamalgam.ingest.BulkManifest
import utest._

/** Resume receipts must describe the exact checked release, never merely an existing filename. */
object BulkManifestSuite extends TestSuite {
  private val projection = "a" * 64

  private def document(checksum: String = "900150983cd24fb0d6963f7d28e17f72"): String =
    s"""release: fixture-v1
       |scope: offline fixture
       |files:
       |  - name: records.json.bz2
       |    url: https://example.org/releases/v1/records.json.bz2
       |    bytes: 3
       |    md5: $checksum
       |    role: active
       |""".stripMargin

  val tests = Tests {
    test("pinned inventory, bytes and digest are mandatory; latest URLs are not release pins") {
      val manifest = BulkManifest.parse(document())
      assert(manifest.release == "fixture-v1")
      assert(manifest.files.size == 1)
      assertThrows[IllegalArgumentException](BulkManifest.parse(document().replace("bytes: 3", "bytes: -1")))
      assertThrows[IllegalArgumentException](BulkManifest.parse(document("bad")))
      assertThrows[IllegalArgumentException](
        BulkManifest.parse(document().replace("/releases/v1/", "/latest_release/"))
      )
      assertThrows[IllegalArgumentException](BulkManifest.parse(document().replace("records.json.bz2", "../records")))
      assertThrows[IllegalArgumentException](BulkManifest.parse(document() + "unexpected: true\n"))
    }

    test("checksum mismatch, truncation and partial projection cannot become a resume receipt") {
      val dir = os.temp.dir()
      try {
        val manifest = BulkManifest.parse(document())
        val entry = manifest.files.head
        val input = dir / entry.name
        os.write(input, "abc")
        BulkManifest.verify(input, entry)
        val output = dir / "projected"
        os.makeDir.all(output)
        assertThrows[IllegalArgumentException](BulkManifest.complete(manifest, entry, input, output, 1, projection))
        os.write(output / "part-00000.parquet", "projected fixture")
        os.write(output / "_SUCCESS", "")
        BulkManifest.complete(manifest, entry, input, output, 1, projection)
        assert(BulkManifest.reusable(manifest, entry, output, projection))
        assert(!BulkManifest.reusable(manifest, entry, output, "b" * 64))
        assert(!BulkManifest.reusable(manifest.copy(release = "fixture-v2"), entry, output, projection))
        os.remove(input)
        assert(
          BulkManifest.reusable(manifest, entry, output, projection)
        ) // source shard may be released after projection
        os.write(input, "abd")
        assertThrows[IllegalArgumentException](BulkManifest.verify(input, entry))
        os.write.over(input, "ab")
        assertThrows[IllegalArgumentException](BulkManifest.verify(input, entry))
        os.remove(output / "part-00000.parquet")
        assert(!BulkManifest.reusable(manifest, entry, output, projection))
      } finally os.remove.all(dir)
    }

    test("checked-in inventories pin every RefSNP partition and one dated Wikidata dump") {
      val dbsnp = BulkManifest.parse(os.read(TestSupport.resource("/config/dbsnp_bulk.yaml")))
      val expected = (1 to 22).map(i => s"refsnp-chr$i.json.bz2").toSet ++
        Set("X", "Y", "MT").map(c => s"refsnp-chr$c.json.bz2") ++
        Set("merged", "withdrawn", "unsupported", "nosnppos", "other").map(c => s"refsnp-$c.json.bz2")
      assert(dbsnp.files.map(_.name).toSet == expected)
      assert(dbsnp.files.map(_.bytes).sum == 485701336617L)
      val wikidata = BulkManifest.parse(os.read(TestSupport.resource("/config/wikidata_bulk.yaml")))
      assert(wikidata.files.size == 1)
      assert(wikidata.files.head.bytes == 103488697311L)
      assert(wikidata.release == "wikidata-20261019")
    }

    test("inventory rejects duplicate names and unsafe resource budgets") {
      val yaml = document()
      val duplicate = yaml + yaml.substring(yaml.indexOf("  - name:"))
      assertThrows[IllegalArgumentException](BulkManifest.parse(duplicate))
      val m = BulkManifest.parse(yaml)
      assertThrows[IllegalArgumentException](
        BulkManifest.requireSpace(100, m.files.head, reserveBytes = 99, outputAllowanceBytes = 0)
      )
      BulkManifest.requireSpace(103, m.files.head, reserveBytes = 99, outputAllowanceBytes = 1)
    }
  }
}
