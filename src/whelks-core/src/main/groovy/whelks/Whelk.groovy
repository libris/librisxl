package se.kb.libris.conch

import java.net.URI

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.BasicWhelk

import se.kb.libris.conch.component.*

class Whelk extends BasicWhelk {
    private def components = []
    def name

    def Whelk(name) {this.name = name}

    def query(def q) {
        println "Whelk ${this.class.name} received query ${q}"
    }

    def ingest(String docString) {

    }

    def addComponent(Component c) {
        c.setWhelk(this)
        this.components.add(c)
    }

    def ingest(MyDocument d) {
        d.type = "marc21"
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
            println "Looping component ${it.class.name}"
            if (it instanceof Storage) {
                println "Is storage. Retrieving ..."
                doc = it.retrieve(identifier)
            }
        }
        return doc
    }

    def find(query) {
        //def response = index.find(this.name, "marc21", identifier)
        def doc = null
        components.each {
            println "Looping component ${it.class.name}"
            if (it instanceof Index) {
                println "Is index. Searching ..."
                doc = it.find(query)
                if (doc != null) {
                    println "Found a ${doc.class.name}"
                }
            }
        }
        println "Located document from elastic search"
        return doc
    }
}
