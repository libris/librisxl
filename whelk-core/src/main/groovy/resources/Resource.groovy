package se.kb.libris.whelks

import se.kb.libris.whelks.exception.*

abstract class Resource {
    @IsMetadata
    URI identifier
    byte[] data
    @IsMetadata
    String contentType
    @IsMetadata
    long size

    void setIdentifier(String uri) {
        try {
            this.identifier = new URI(uri)
        } catch (java.net.URISyntaxException e) {
            throw new WhelkRuntimeException(e)
        }
    }

    void setIdentifier(URI uri) {
        this.identifier = uri
    }

    def withData(String dataString) {
        return withData(dataString.getBytes("UTF-8"))
    }

    def withIdentifier(String uri) {
        try {
            this.identifier = new URI(uri)
        } catch (java.net.URISyntaxException e) {
            throw new WhelkRuntimeException(e)
        }
        return this
    }

    def withIdentifier(URI uri) {
        this.identifier = uri
        return this
    }

    def withData(byte[] data) {
        this.data = data
        this.size = data.length
        return this
    }

    def withContentType(String contentType) {
        this.contentType = contentType
        return this
    }

    String getDataAsString() {
        return new String(this.data)
    }

    Map getDataAsMap() {
        return mapper.readValue(this.data, Map)
    }
}
