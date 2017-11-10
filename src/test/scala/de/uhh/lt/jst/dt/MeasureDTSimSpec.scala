package de.uhh.lt.jst.dt

import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.uhh.lt.jst.dt.benchmarks.MeasureDTSim
import de.uhh.lt.jst.dt.entities.DTEntry
import org.scalatest._

class MeasureDTSimSpec extends FlatSpec with Matchers with DatasetSuiteBase {
  it should "measure similarity between two small distributional thesauri" in {

    import spark.implicits._

    val commonEntries = Seq(
      DTEntry("term1", "term11", 1),
      DTEntry("term1", "term12", 1),
      DTEntry("term2", "term21", 1),
      DTEntry("term2", "term22", 1)
    )

    val dt1 = (commonEntries ++ Seq(
      DTEntry("term1", "term13", 1),
      DTEntry("term2", "term23", 1)
    )).toDS()

    val dt2 = (commonEntries ++ Seq(
      DTEntry("term1", "term14", 1)
    )).toDS()

    val result = MeasureDTSim.compareDTs(dt1, dt2)

    val expected = MeasureDTSim.Measurements(
      numCommon = 4L,
      numExtra = 2L,
      numMissing = 1L
    )

    result should be(expected)
  }

}
