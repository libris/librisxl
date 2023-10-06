package whelk.component

import whelk.Document
import whelk.filter.BlankNodeLinker

interface DocumentNormalizer {
    default BlankNodeLinker getLinker() {}

    void normalize(Document doc)
}