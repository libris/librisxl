package whelk.importer


import sun.reflect.generics.reflectiveObjects.NotImplementedException
import whelk.Document
import whelk.IdGenerator
import whelk.Whelk
import whelk.reindexer.CardRefresher

import static whelk.util.Jackson.mapper

class DefinitionsImporter extends Importer {
    String definitionsFilename

    DefinitionsImporter(Whelk w) {
        whelk = w
    }

    void doImport(String collection) {
        long startTime = System.currentTimeMillis()
        File defFile = new File(definitionsFilename)
        List documentList = []
        int counter = 0
        defFile.eachLine {
            def data = mapper.readValue(it.getBytes("UTF-8"), Map)
            Document doc = new Document(data)
            ensureAbsoluteSystemId(doc)
            documentList.add(doc)
            counter++
        }
        println("Created $counter documents from $definitionsFilename in ${(System.currentTimeMillis() - startTime) / 1000} seconds. Now storing to system.")
        whelk.storage.bulkStore(documentList, "xl", null, collection)
        println("Operation complete. Time elapsed: ${(System.currentTimeMillis() - startTime) / 1000} seconds.")

        whelk.loadCoreData()
        new CardRefresher(whelk).refresh(collection)
    }

    @Override
    ImportResult doImport(String collection, String sourceSystem, Date from) {
        throw new NotImplementedException()
    }
    void ensureAbsoluteSystemId(Document doc) {
        def sysBaseIri = Document.BASE_URI
        // A system id is in place; do nothing and return.
        if (doc.recordIdentifiers.any { it.startsWith(sysBaseIri.toString()) }) {
            return
        }
        // A relative system id (slug) exists; make it absolute and return.
        def slug = doc.recordIdentifiers.find { it =~ /^\w+$/ }
        if (slug) {
            doc.setId(slug)
            return
        }
        // Mint a new system Id and set it.
        def newId = sysBaseIri.resolve(IdGenerator.generate()).toString()
        doc.addRecordIdentifier(newId)
    }
}
