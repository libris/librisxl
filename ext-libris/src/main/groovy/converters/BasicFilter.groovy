package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.Document

abstract class BasicFilter extends BasicPlugin implements Filter {

    final Document filter(final Document doc) {
        Document newdoc = doc
        if (doc.contentType == requiredContentType) {
            newdoc = doFilter(doc)
        }
        return newdoc
    }

    abstract Document doFilter(final Document doc)
}
