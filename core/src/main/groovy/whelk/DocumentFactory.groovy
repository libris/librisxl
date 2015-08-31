package whelk

import whelk.exception.*

class DocumentFactory {
    ////@Override
    Document createDocument(String contentType) {
        if (contentType == "application/ld+json") {
            return new JsonLdDocument().withContentType(contentType)
        } else if (contentType ==~ /application\/(\w+\+)*json/ || contentType ==~ /application\/x-(\w+)-json/) {
            return new JsonDocument().withContentType(contentType)
        } else {
            return new DefaultDocument().withContentType(contentType)
        }
    }

    //@Override
    Document createDocumentFromJson(String json) {
        try {
            Document document = mapper.readValue(json, DefaultDocument)
            if (document.isJson()) {
                if (document.contentType == "application/ld+json") {
                    return new JsonLdDocument().fromDocument(document)
                } else {
                    return new JsonDocument().fromDocument(document)
                }
            }
            return document
        } catch (org.codehaus.jackson.JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
    }

    JsonDocument createDocument(Map data, Map manifest) {
        return new JsonLdDocument().withData(data).withManifest(manifest)
    }

    //@Override
    Document createDocument(byte[] data, Map manifest, Map meta) {
        Document document = new DefaultDocument().withData(data).withMeta(meta).withManifest(manifest)
        if (document.isJson()) {
            if (document.contentType == "application/ld+json") {
                return new JsonLdDocument().fromDocument(document)
            } else {
                return new JsonDocument().fromDocument(document)
            }
        }
        return document
    }

    //@Override
    Document createDocument(Map data, Map manifest, Map meta) {
        Document document = new JsonDocument().withData(data).withMeta(meta).withManifest(manifest)
        if (document.contentType == "application/ld+json") {
            return new JsonLdDocument().fromDocument(document)
        } else {
            return new JsonDocument().fromDocument(document)
        }
        return document
    }
}
