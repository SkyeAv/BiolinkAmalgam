package biolinkamalgam

import biolinkamalgam.transform.NameKey
import utest._

/** Pure match-key tests — no Spark session needed. Only strip/clean: this codebase does no stemming or level keys (real
  * NLP is downstream's job).
  */
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

    test("clean strips matching and duplicate quotes, then lowercases") {
      assert(NameKey.clean("\"aspirin\"") == "aspirin")
      assert(NameKey.clean("'aspirin'") == "aspirin")
      // duplicate leading quote drops ONE char per fixpoint round
      assert(NameKey.clean("''aspirin") == "'aspirin")
      assert(NameKey.clean("  \"Quoted Name\"  ") == "quoted name")
      assert(NameKey.clean(null) == "")
    }

  }
}
