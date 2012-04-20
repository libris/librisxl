package se.kb.libris.conch

import java.net.URI
import org.restlet.*

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.BasicWhelk

import se.kb.libris.conch.component.*

class Whelk extends BasicWhelk {
    def components = []
    def name

    def Whelk(name) {this.name = name}

    def query(def q) {
        println "Whelk ${this.class.name} received query ${q}"
    }

    def ingest(String docString) {

    }

    def ingest(MyDocument d) {
        components.each {
            it.add(d)
        }
        /*
        storage.store(d)
        index.index(d, this.name, d.type)
        */
        return d.identifier
    }

    def retrieve(identifier) {
        identifier = new URI(identifier)
        def doc = null
        components.each {
            println "Looping component ${it.class.name}"
            if (it instanceof Storage) {
                println "Is storage. Retrieving ..."
                doc = it.retrieve(identifier)
                println "Found a ${doc.class.name}"
            }
        }
        return doc
    }

    def find(identifier) {
        def response = index.find(this.name, "marc21", identifier)
        println "Located document from elastic search"
        println response
    }
}
