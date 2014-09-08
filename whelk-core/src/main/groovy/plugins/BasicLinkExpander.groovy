package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.Document

abstract class BasicLinkExpander extends BasicPlugin implements LinkExpander {

    Document transmogrify(Document document) {
        return expand(document)
    }

    final Document expand(Document document) {
        if (valid(document)) {
            return doExpand(document)
        }
        return document
    }

    abstract boolean valid(Document doc)
    abstract Document doExpand(Document doc)
}
