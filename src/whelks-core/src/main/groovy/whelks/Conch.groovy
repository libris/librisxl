package se.kb.libris.conch

class Document {}
class Storage {
    def store(Document d) {}
}
class Index {}
class TripleStore {}

class Whelk {
    Storage s
    Index i
    TripleStore ts

    Whelk(Storage _s, Index _i, TripleStore _ts) { s = _s; i = _i; ts = _ts}
    Whelk(Storage _s, Index _i) {s = _s; i = _i}
    Whelk(Storage _s) {s = _s}

    def query(def q) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}

class API {
    def whelks = []

    def query(def q) {
        def i = 0
        whelks.each{
            println "whelk number ${i}: ${it}"
            (${it}).query(q)
            i++
        }
    }

    def addWhelk(Whelk whelk) {
        whelks << whelk
    }
}

class App {
    static main(args) {
        def storage = new Storage()
        def whelk = new Whelk(storage)

        def api = new API()
        api.addWhelk(whelk)


        api.query('Fragile Things')
    }
}
