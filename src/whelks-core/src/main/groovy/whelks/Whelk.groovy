package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.TeeOutputStream

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.api.RestAPI
import se.kb.libris.whelks.basic.BasicWhelk
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.Plugin

import se.kb.libris.conch.data.WhelkDocument
import se.kb.libris.conch.data.WhelkSearchResult

@Log
class WhelkImpl extends BasicWhelk {

    private def plugins = []
    private def apis = []
    def name
    def defaultIndex

    def WhelkImpl(name) { setName(name) }

    def setName(n) {
        this.name = n
        this.defaultIndex = n
    }

    def URI store(String docString, String contentType) {
        Document d = createDocument().withURI(generate_identifier()).withContentType(contentType).withData(docString.getBytes())
        return store(d)
    }

    def URI generate_identifier() {
        def uri = _create_random_URI()
        while (has_identifier(uri)) {
            uri = _create_random_URI()
        }
        return uri
    }

    boolean isBinaryData(byte[] data) {
        return true
    }

    def has_identifier(uri) {
        // TODO: implement properly
        return false
    }

    def _create_random_URI() {
        def generator = { String alphabet, int n ->
            new Random().with {
                (1..n).collect { alphabet[ nextInt( alphabet.length() ) ] }.join()
            }
        }
        return new URI("/" + this.name + "/" + generator( (('A'..'Z')+('a'..'z')+('0'..'9')).join(), 8 ))
    }

    @Override
    def void addPlugin(Plugin p) {
        p.setWhelk(this)
        this.plugins.add(p)
    }

    def addAPI(RestAPI a) {
        a.setWhelk(this)
        this.apis.add(a)
    }

    def getApis() {
        return this.apis
    }


    def getStorages() {
        def storages = []
        plugins.each {
            if (it instanceof Storage) {
                storages << it
            }
        }
        return storages
    }

    def URI store(String contentType, InputStream is, long size = -1) {
    }

    @Override
    def URI store(Document doc) {
        return store(doc.identifier, doc.contentType, doc.dataAsStream)
    }

    def URI store(URI identifier, String contentType, InputStream is, long size = -1) {
        log.debug("Storing ${identifier} with ctype $contentType")
        def combinedOutputStream = null
        storages.each { 
            def os = it.getOutputStreamFor(identifier, contentType)
            if (! combinedOutputStream) {
                combinedOutputStream = os
            } else {
                combinedOutputStream = new TeeOutputStream(combinedOutputStream, os) 
            }
        }
        try {
            long savedBytes = IOUtils.copyLarge(is, combinedOutputStream)
            if (size != -1 && savedBytes != size) {
                throw new WhelkRuntimeException("Expected $size bytes. Received $savedBytes.")
            }
        } finally {
            combinedOutputStream.close()
        }
        return identifier
    }

    @Override
    def Document get(identifier, raw=false) {
        if (identifier instanceof String) {
            identifier = new URI(identifier)
        }
        def doc = null
        plugins.each {
            if (it instanceof Storage) {
                log.debug "${it.class.name} is storage. Retrieving ..."
                doc = it.retrieve(identifier, raw)
            }
        }
        if (doc == null) {
            throw new WhelkRuntimeException("Document not found: $identifier")
        }
        return doc
    }


    @Override
    def SearchResult query(String query, boolean raw = false) {
        return new WhelkSearchResult(find(query, raw))
    }

    def find(query, raw = false) {
        def doc = null
        plugins.each {
            log.debug "Looping component ${it.class.name}"
            if (it instanceof GIndex) {
                log.debug "Is index. Searching ..."
                doc = it.find(query, this.defaultIndex, raw)
                if (doc != null) {
                    log.debug "Found a ${doc.class.name}"
                }
            }
        }
        log.debug "Located document from elastic search"
        return doc
    }

    @Override
    Document createDocument() {
        return new WhelkDocument()
    }
}
