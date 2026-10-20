package biolinkamalgam

import biolinkamalgam.transform.NameKey
import utest._

/** Pure match-key tests — no Spark session needed. */
object NameKeySuite extends TestSuite {

  val tests = Tests {

    test("strip lowercases, trims, collapses whitespace") {
      assert(NameKey.strip("  Examplein  ") == "examplein")
      assert(NameKey.strip("EXA   Protein") == "exa protein")
    }

    test("strip drops non-word chars") {
      assert(NameKey.strip("Glucose-6-Phosphatase") == "glucose 6 phosphatase")
      assert(NameKey.strip("G6PC1 (Hsap)") == "g6pc1 hsap")
    }

    test("strip keeps underscores (word chars)") {
      assert(NameKey.strip("a_b") == "a_b")
    }

    test("strip handles null and junk") {
      assert(NameKey.strip(null) == "")
      assert(NameKey.strip("!!!") == "")
    }

    test("preferred names match as written, never stemmed") {
      // strip-only equality is what CliqueMerge uses: plurals must NOT merge
      assert(NameKey.strip("glucoses") != NameKey.strip("glucose"))
      // but case/whitespace/punctuation variants must
      assert(NameKey.strip(" examplein") == NameKey.strip("Examplein"))
    }

    test("stem collides inflections for arbitration") {
      assert(NameKey.stem("glucoses") == NameKey.stem("glucose"))
      assert(NameKey.stem("EXA Protein") == NameKey.stem("exa proteins"))
    }

    test("stem normalizes multi-token names token-wise") {
      assert(NameKey.stem("catalytic subunits") == NameKey.stem("Catalytic Subunit"))
    }

    test("stem of empty/junk is empty") {
      assert(NameKey.stem("") == "")
      assert(NameKey.stem(null) == "")
    }
  }
}
