package utils

import org.scalatest._

class UtilTest extends FlatSpec with Matchers {
  "The Utils object" should "split dependency features" in {
    Util.parseDep("subj(work,@)") should equal(("subj", "work", "@"))
  }

  it should "split dependency features 2" in {
    Util.parseDep("obj(@,man)") should equal(("obj", "@", "man"))
  }

  it should "split trigram features" in {
    Util.parseTrigram("touring_@_championship") should equal(("touring", "championship"))
  }

  it should "split trigram features 2" in {
    Util.parseTrigram("sport_@_,") should equal(("sport", ","))
  }

}
