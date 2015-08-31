package whelk

import whelk.exception.*

class DocumentFactory {
    Document createDocument(Map data, Map manifest, Map meta) {
        return new Document().withData(data).withManifest(manifest)
    }
}
