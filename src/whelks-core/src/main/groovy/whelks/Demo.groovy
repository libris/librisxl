package se.kb.libris.whelks

import java.util.HashMap
import java.net.URI
import se.kb.libris.whelks.persistance.Initialisable

class MyWhelkFactory extends WhelkFactory {

    MyWhelkFactory() {
        println "Instantiating " + this.class.name
    }

    Whelk create() {
        new MyWhelk()
    }

    static main(args) {
        def map = new HashMap()
        map.put('classname', 'se.kb.libris.whelks.MyWhelkFactory') 
        def factory = WhelkFactory.newInstance(map)
        def whelk = factory.create()
    }
}


class MyWhelk implements Whelk {

    URI store(Document d) {}
    URI store(URI uri, Document d) {}
    Document get(URI uri) {}
    void delete(URI uri) {}

    SearchResult query(String query) {}
    LookupResult lookup(Key key) {}

    void destroy() {}

    Document createDocument(String contentType, String format, byte[] data) {}
}

