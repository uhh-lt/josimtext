/**
 * Utility functions
 */
object Util {
    /**
     * Splits a string into at most n parts, given del as delimitor. Occurences of del are handled from right to left,
     * leaving out remaining occurrences if n splits have already been found.
     * @param text String to split
     * @param del Delimiter
     * @param n Maximum number of splits
     * @return String split into at most n parts
     */
    def splitLastN(text:String, del:String, n:Int):Array[String] = {
        val featureArr = new Array[String](3)
        var lastPos = text.length
        for (i <- (0 to n-1).reverse) {
            // for the first element we position an imaginary delimeter at position -1
            val nextPos = if (i == 0) -1 else text.lastIndexOf(del, lastPos-1)
            featureArr(i) = text.substring(nextPos+1, lastPos)
            lastPos = nextPos
        }
        featureArr
    }
}
