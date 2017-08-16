package de.tudarmstadt.lt.conll

case class Row(
  id: String, // Word index, integer starting at 1 for each new sentence;
  // may be a range for multiword tokens; may be a decimal number for empty nodes.
  form: String, // Word form or punctuation symbol.
  lemma: String, // Lemma or stem of word form.
  upostag: String, // Universal part-of-speech tag.
  xpostag: String, // Language-specific part-of-speech tag; underscore if not available.
  feats: String, // List of morphological features from the universal feature inventory or
  // from a defined language-specific extension; underscore if not available.
  head: String, // Head of the current word, which is either a value of ID or zero (0).
  deprel: String, // Universal dependency relation to the HEAD (root iff HEAD = 0)
  // or a defined language-specific subtype of one.
  deps: String, // Enhanced dependency graph in the form of a list of head-deprel pairs.
  misc: String // Any other annotation.
)