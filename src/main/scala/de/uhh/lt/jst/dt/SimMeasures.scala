package de.uhh.lt.jst.dt

/**
  * Similarity measures as proposed in Biemann C., Text: now in 2D!, 2013
  */
object SimMeasures {

  type SimMeasureFunc = (Long, Long, Long, Long) => Double

  def log2(n: Double): Double = {
    math.log(n) / math.log(2)
  }

  /**
    * Computes a log-likelihood ratio approximation
    *
    * @param n    Total number of observations
    * @param n_A  Number of times A was observed
    * @param n_B  Number of times B was observed
    * @param n_AB Number of times A and B were observed together
    */
  def ll(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    val wcL = log2(n_A)
    val fcL = log2(n_B)
    val bcL = log2(n_AB)
    val epsilon = 0.000001
    val res = 2 * (n * log2(n)
      - n_A * wcL
      - n_B * fcL
      + n_AB * bcL
      + (n - n_A - n_B + n_AB) * log2(n - n_A - n_B + n_AB + epsilon)
      + (n_A - n_AB) * log2(n_A - n_AB + epsilon)
      + (n_B - n_AB) * log2(n_B - n_AB + epsilon)
      - (n - n_A) * log2(n - n_A + epsilon)
      - (n - n_B) * log2(n - n_B + epsilon))
    if ((n * n_AB) < (n_A * n_B)) -res.toDouble else res.toDouble
  }

  /**
    * Computes the lexicographer's mutual information (LMI) score:<br/>
    *
    * <pre>LMI(A,B) = n_AB * log2( (n*n_AB) / (n_A*n_B) )</pre>
    * <br/>
    * Reference:
    * Kilgarri, A., Rychly, P., Smrz, P., Tugwell, D.: The sketch engine. In: <i>Proceedings of Euralex</i>, Lorient, France (2004) 105-116
    *
    * @param n    Total number of observations
    * @param n_A  Number of times A was observed
    * @param n_B  Number of times B was observed
    * @param n_AB Number of times A and B were observed together
    */
  def lmi(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    n_AB * (log2(n * n_AB) - log2(n_A * n_B))
  }

  def cov(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    n_AB.toDouble / n_A.toDouble
  }

  def freq(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    n_AB.toDouble
  }
}
