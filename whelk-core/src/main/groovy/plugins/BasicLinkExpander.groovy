package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.Document

abstract class BasicLinkExpander extends BasicPlugin implements LinkExpander {

    final Document expand(Document document) {
        if (validDocument(document)) {
            return doExpand(document)
        }
        return document
    }

    abstract boolean validDocument(Document doc)
    abstract Document doExpand(Document doc)
}
