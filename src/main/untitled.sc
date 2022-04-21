val myMap = "helllo hello world".split(" +").foldLeft(Map.empty[String, Int]) {
  (words, w) => words + (w -> (words.getOrElse(w, 0) + 1))
}

myMap.toString()