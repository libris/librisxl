package se.kb.libris.conch

import groovy.util.logging.Slf4j as Log

import java.net.URI

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.api.RestAPI
import se.kb.libris.whelks.basic.BasicWhelk
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.Plugin

import se.kb.libris.conch.data.MyDocument
/*
import se.kb.libris.conch.plugin.*

*/

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

    def query(def q) {
        log.debug "Whelk ${this.class.name} received query ${q}"
    }

    def ingest(String docString) {
        MyDocument d = new MyDocument(generate_identifier()).withData(docString.getBytes())
        ingest(d)
    }

    def generate_identifier() {
        def uri = _create_random_URI()
        while (has_identifier(uri)) {
            uri = _create_random_URI()
        }
        return uri
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

    def ingest(Document d) {
        def responses = [:]
        plugins.each {
            if (it instanceof Component) {
                responses.put(it.class.name, it.add(d))
            }
        }
        return responses
    }

    def retrieve(identifier, raw=false) {
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

    def find(query, raw = false) {
        def doc = null
        plugins.each {
            log.debug "Looping component ${it.class.name}"
            if (it instanceof Index) {
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
}
