package biolinkamalgam

import biolinkamalgam.transform.DisplayNames
import utest._

object DisplayNamesSuite extends TestSuite {
  private val spark = TestSupport.spark
  val tests = Tests {
    test("promote every identifier and replace only CURIE-like or missing preferred names") {
      import spark.implicits._
      val input = Seq(
        ("TEST:1", "TEST:1", Seq("Zeta", "Alpha", "unknown", "0.5", "OTHER:2"), Seq("OTHER:2")),
        ("TEST:2", "Curated name", Seq("Alpha"), Seq("OTHER:3")),
        ("TEST:3", "TEST:3", Seq("OTHER:4", "unknown"), Seq("OTHER:4")),
        ("TEST:4", null, Seq.empty[String], Seq.empty[String]),
        ("TEST:5", "TEST:5", Seq("  Human label  "), Seq.empty[String])
      ).toDF("leader", "preferred_name", "names", "identifiers")
      val rows =
        DisplayNames(input, Set("*"), TestSupport.filters).collect().map(r => r.getAs[String]("leader") -> r).toMap
      assert(rows("TEST:1").getAs[String]("preferred_name") == "Alpha")
      assert(rows("TEST:2").getAs[String]("preferred_name") == "Curated name")
      assert(rows("TEST:3").getAs[String]("preferred_name") == "TEST:3")
      assert(rows("TEST:4").getAs[String]("preferred_name") == null)
      assert(rows("TEST:5").getAs[String]("preferred_name") == "  Human label  ")
      assert(rows("TEST:1").getAs[Seq[String]]("names").contains("TEST:1"))
      assert(rows("TEST:2").getAs[Seq[String]]("names").contains("OTHER:3"))
      val restricted = DisplayNames(input, Set("OTHER"), TestSupport.filters).filter("leader = 'TEST:2'").head()
      assert(!restricted.getAs[Seq[String]]("names").contains("TEST:2"))
      assert(restricted.getAs[Seq[String]]("names").contains("OTHER:3"))
    }
  }
}
