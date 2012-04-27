package se.kb.libris.conch

import groovy.util.logging.Slf4j as Log

import java.net.URI

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.BasicWhelk

import se.kb.libris.conch.component.*
import se.kb.libris.conch.data.*

@Log
class Whelk extends BasicWhelk {
    private def components = []
    def name

    def Whelk(name) {this.name = name}

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

    def addComponent(Component c) {
        c.setWhelk(this)
        this.components.add(c)
    }

    def ingest(Document d) {
        def responses = [:]
        components.each {
            responses.put(it.class.name, it.add(d))
        }
        return responses
    }

    def retrieve(identifier) {
        if (identifier instanceof String) {
            identifier = new URI(identifier)
        }
        def doc = null
        components.each {
            log.debug "Looping component ${it.class.name}"
            if (it instanceof Storage) {
                log.debug "Is storage. Retrieving ..."
                doc = it.retrieve(identifier)
            }
        }
        return doc
    }

    def find(query) {
        //def response = index.find(this.name, "marc21", identifier)
        def doc = null
        components.each {
            log.debug "Looping component ${it.class.name}"
            if (it instanceof Index) {
                log.debug "Is index. Searching ..."
                doc = it.find(query)
                if (doc != null) {
                    log.debug "Found a ${doc.class.name}"
                }
            }
        }
        log.debug "Located document from elastic search"
        return doc
    }
}
