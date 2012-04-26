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

    }

    def addComponent(Component c) {
        c.setWhelk(this)
        this.components.add(c)
    }

    def ingest(MyDocument d) {
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
