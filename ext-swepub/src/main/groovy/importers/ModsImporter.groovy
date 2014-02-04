package se.kb.libris.whelks.swepub.importers

import se.kb.libris.whelks.importers

class ModsImporter extends DumpImporter {

    @Override
    Document buildDocument(String xmlData) {
        return new Document().withData(xmlData.getBytes("UTF-8")).withEntry(["contentType":"application/mods+xml"])
    }
}
