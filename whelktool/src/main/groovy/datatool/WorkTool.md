* language Swedish
* Fiction
* issuanceType Monograph
* No hasPart
* encodingLevel is not marc:PartialPreliminaryLevel or marc:PrepublicationLevel
  TODO: specify a minimal set of properties that must exist?


fiction
-------



properties
----------

* **classification** Always take the sum of all works.
  * SAB/kssb - Merge codes that are the same or prefixes. Take the longer code. Take the latest SAB version. Example: kssb/8 Hc + kssb/7 Hc.02 = kssb/8 Hc.02
  * Dewey - Merge equal codes with different editionEnumeration, use the newest.
* **contentType** Allow missing or `https://id.kb.se/term/rda/Text`
* **subject** Always take the sum of all works. 
  * TODO: preserve order?
* **hasTitle** Take from one random work. 
  * TODO: Take the most common one? Some other metric of "best"?
* **genreForm** Take from all works. Only keep the right one if both occur of the following:
  * marc/NotFictionNotFurtherSpecified -> marc/FictionNotFurtherSpecified (i.e. actually fiction)
  * marc/FictionNotFurtherSpecified -> marc/Novel
  * marc/FictionNotFurtherSpecified -> marc/Poetry
  * marc/NotFictionNotFurtherSpecified -> marc/Autobiography
  * marc/NotFictionNotFurtherSpecified -> marc/Biography

Instance properties
* **editionStatement** Added to comparison if it contains "förk" (förkortad = abbreviated). Then it must be the exact same string.
* **extent** Number of pages parsed from extent may not differ more than 30%. 
  * TODO: allow missing extent?