package se.kb.libris.conch

import se.kb.libris.whelks.Document
import java.net.URI

class MyDocument implements Document {
    URI identifier
    List links
    List keys
    byte[] data
    String contentType
    String format
    long size

    MyDocument(String s) {
        data = s.getBytes()
    }

    Document withData(byte[] data) {}
    Document withFormat(String format) {}
    Document withContentType(String contentType) {}
}

class Storage {
    def storageDir = "./storage/"

    Storage() {
        init()
    }

    Storage(def directoryName) {
        setStorageDir(directoryName)
    }

    def init() {
        new File(storageDir).mkdirs()
    }

    def setStorageDir(String directoryName) { 
        storageDir = directoryName 
        init()
    }

    def store(Document d) {
        def filename = (d.identifier ? d.identifier.toString() : _create_filename())
        File file = new File("$storageDir/$filename").withWriter {
            out -> 
                out.println d.data
        }
    }

    def _create_filename() {
        def pool = ['a'..'z','A'..'Z',0..9,'_'].flatten()
        Random rand = new Random(System.currentTimeMillis())

        def passChars = (0..10).collect { pool[rand.nextInt(pool.size())] }
        passChars.join()
    }
}

class Index {}
class TripleStore {}

class Whelk {
    Storage storage
    Index index
    TripleStore ts

    Whelk(Storage _s, Index _i, TripleStore _ts) { storage = _s; index = _i; ts = _ts}
    Whelk(Storage _s, Index _i) {storage = _s; index = _i}
    Whelk(Storage _s) {storage = _s}

    def query(def q) {
        println "Whelk ${this.class.name} received query ${q}"
    }

    def ingest(Document d, boolean bulk = false) {
        storage.store(d)
    }

    def retrieve(URI identifier) {}
}

class API {
    def whelks = []

    def query(def q) {
        whelks.eachWithIndex { whelk, i ->
            println "Asking whelk ${i}: ${whelk}"
            whelk.query(q)
        }
    }

    def addWhelk(Whelk whelk) {
        whelks << whelk
    }
}

class App {
    static main(args) {
        def env = System.getenv()
        def whelk_storage = (env['PROJECT_HOME'] ? env['PROJECT_HOME'] : System.getProperty('user.home')) + "/whelk_storage"
        def storage = new Storage(whelk_storage)
        def whelk = new Whelk(storage)

        def api = new API()
        api.addWhelk(whelk)

        /*
        api.query('Fragile Things')
        */

        if (args.length > 0) {
            def file = new File(args[0])
            println "Loading file ${file}"
            println "${file.text}"
            def doc = new MyDocument(file.text)
            def identifier = whelk.ingest(doc)
            println "Stored document with identifier ${identifier}"
        }
    }
}
