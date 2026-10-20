package biolinkamalgam

import biolinkamalgam.transform.NameKey
import utest._

/** Pure match-key tests — no Spark session needed. Anchors marked [fullmap] are the exact documented examples from
  * Tablassert's nlp.py/`normalize_l1` docstrings; the implementation must agree with them.
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

    test("l1 folds token order, duplicates, and casing [fullmap]") {
      val a = NameKey.l1("aspirin oral")
      assert(a == NameKey.l1("oral aspirin"))
      assert(a == NameKey.l1("Aspirin ORAL"))
      assert(a == NameKey.l1("  aspirin aspirin   oral "))
    }

    test("l1 stems all-ASCII-alphabetic tokens with Porter2 [fullmap]") {
      assert(NameKey.l1("genes") == "gene")
      assert(NameKey.l1("acetic") == "acet")
      assert(NameKey.l1("glucoses") == NameKey.l1("glucose"))
      assert(NameKey.l1("catalytic subunits") == NameKey.l1("Catalytic Subunit"))
    }

    test("l1 passes tokens with digits/punctuation through unstemmed [fullmap]") {
      assert(NameKey.l1("51") == "51")
      assert(NameKey.l1("tnf-alpha") == "tnf-alpha")
      assert(NameKey.l1("bcl2") == "bcl2")
    }

    test("l1 sorts tokens code-point-wise") {
      // [fullmap] rust nlp.rs: normalize_l1("oral aspirin") == "aspirin oral"
      assert(NameKey.l1("oral aspirin") == "aspirin oral")
      val key = NameKey.l1("subunit catalytic")
      assert(key == NameKey.l1("catalytic subunit"))
      assert(key.split(" ").toSeq == key.split(" ").toSeq.sorted)
    }

    test("l1 of empty/junk is empty") {
      assert(NameKey.l1("") == "")
      assert(NameKey.l1(null) == "")
    }

    test("l2 keeps only [a-z0-9_]") {
      assert(NameKey.l2("tnf-alpha") == "tnfalpha")
      assert(NameKey.l2("G6PC1 (Hsap)") == "g6pc1hsap")
      assert(NameKey.l2("p_value") == "p_value")
      assert(NameKey.l2(null) == "")
    }

    test("l2 is not stemmed, unlike l1") {
      // fullmap dedup exploits exactly this difference
      assert(NameKey.l1("glucose") == "glucos")
      assert(NameKey.l2("glucose") == "glucose")
      // for an already-atomic token they coincide (L2 null in the output)
      assert(NameKey.l1("g6pc1") == NameKey.l2("g6pc1"))
    }
  }
}
