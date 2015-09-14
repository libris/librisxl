package whelk

import whelk.exception.*

@Deprecated
class DocumentFactory {
    Document createDocument(Map data, Map manifest) {
        return new Document().withData(data).withManifest(manifest)
    }
}
