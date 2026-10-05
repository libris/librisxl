package whelk.component

import groovy.transform.CompileStatic
import whelk.Document
import whelk.filter.BlankNodeLinker

@CompileStatic
interface DocumentNormalizer {
    default BlankNodeLinker getLinker() {}

    void normalize(Document doc)
}