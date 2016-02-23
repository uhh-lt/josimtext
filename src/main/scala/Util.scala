import java.io.{IOException, File}
import scala.io.{BufferedSource, Source}
import org.apache.spark.SparkContext
import java.nio.file.{Paths, Files}

/**
 * Utility functions
 */
object Util {


  def exists(Path:String) = {
      Files.exists(Paths.get(Path))
  }


  /**
    * Parses a trigram feature e.g. "passenger_@_." into the tuple of strings (passenger, .)
    * */
    def parseTrigram(trigramFeature: String) = {
      val fields = trigramFeature.split("_")

      if (fields.size == 3) {
          (fields(0), fields(2))
      } else {
          ("?", "?")
      }
    }


    /**
      * Parses dependency line "subj(@,dog)" into the tuple of strings (subj, @, dog)
      * */
    def parseDep(depFeature: String) = {
        val depRest = depFeature.split("\\(")
        if (depRest.size == 2) {
            val depType = depRest(0)
            val params = depRest(1).split(",")
            if (params.size == 2){
                val left = params(1).substring(0, params(1).length-1)
                val right = params(0)
                (depType, right, left)
            } else {
                ("?", "?", "?")
            }
        } else {
            ("?", "?", "?")
        }
    }

    def getStopwords() = {
      val source: BufferedSource = Source.fromURL(getClass.getResource(Const.Resources.STOPWORDS))
      source.getLines.toSet
    }

    def loadVocabulary(sc: SparkContext, vocPath: String, lowercase:Boolean=true) = {
      sc.textFile(vocPath)
        .map{ line => line.split("\t")}
        .map{ case Array(word) => if (lowercase) word.toLowerCase() else word }
        .collect()
        .toSet
    }


    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) {
          throw new IOException("Failed to list files for dir: " + file)
        }
        files
      } else {
        List()
      }
    }

    def delete(filePath: String) {
      deleteFile(new File(filePath))
    }

    def deleteFile(file: File) {
      if (file != null) {
        try {
          if (file.isDirectory ) {
            var savedIOException: IOException = null
            for (child <- listFilesSafely(file)) {
              try {
                deleteFile(child)
              } catch {
                case ioe: IOException => savedIOException = ioe
              }
            }
            if (savedIOException != null) {
              throw savedIOException
            }

          }
        } finally {
          if (!file.delete()) {
            if (file.exists()) {
              throw new IOException("Failed to delete: " + file.getAbsolutePath)
            }
          }
        }
      }
    }


    /**
     * Splits a string into at most n parts, given del as delimitor. Occurences of del are handled from right to left,
     * leaving out remaining occurrences if n splits have already been found.
     * @param text String to split
     * @param del Delimiter
     * @param n Maximum number of splits
     * @return String split into at most n parts
     */
    def splitLastN(text:String, del:Char, n:Int):Array[String] = {
        val numSplits = math.min(text.count(_ == del) + 1, n)
        val splits = new Array[String](numSplits)
        var lastPos = text.length
        for (i <- (0 to numSplits-1).reverse) {
            // for the first element we position an imaginary delimeter at position -1
            val nextPos = if (i == 0) -1 else text.lastIndexOf(del, lastPos-1)
            splits(i) = text.substring(nextPos+1, lastPos)
            lastPos = nextPos
        }
        splits
    }
}
