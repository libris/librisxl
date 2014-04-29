package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*

class JsonLDLinkExpander extends BasicLinkExpander {

    List nodesToExpand

    JsonLDLinkExpander(Map settings) {
        this.nodesToExpand = settings['nodesToExpand']
    }

    boolean validDocument(Document doc) {
        if (doc && doc.isJson() && doc.contentType == "application/ld+json") {
            return true
        }
        return false
    }

    Document doExpand(Document doc) {
    }
}
