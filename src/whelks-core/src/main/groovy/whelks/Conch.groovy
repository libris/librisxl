package se.kb.libris.conch

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.storage.Storage
import se.kb.libris.whelks.basic.BasicWhelk
import java.net.URI

class MyDocument extends BasicDocument {
}

class DiskStorage { //implements Storage {
    def storageDir = "./storage/"

    DiskStorage() {
        init()
    }

    DiskStorage(def directoryName) {
        setStorageDir(directoryName)
    }

    def init() {
        new File(storageDir).mkdirs()
    }

    def setStorageDir(String directoryName) { 
        println "Callin setStorageDir"
        storageDir = directoryName 
        init()
    }

    void store(MyDocument d) {
        def filename = (d.identifier ? d.identifier.toString() : _create_filename())
        File file = new File("$storageDir/$filename")
        file.write(new String(d.data))
        d.identifier = new URI(filename)
    }

    MyDocument retrieve(URI u) {
        def s 
        def filename = u.toString()
        File f = new File("$storageDir/$filename")
        println "Filecontents:"
        println f.text
        return new MyDocument(f.text)
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

class Whelk extends BasicWhelk {
    DiskStorage storage
    Index index
    TripleStore ts

    Whelk(Storage _s, Index _i, TripleStore _ts) { storage = _s; index = _i; ts = _ts}
    Whelk(Storage _s, Index _i) {storage = _s; index = _i}
    Whelk(DiskStorage _s) {storage = _s}

    def query(def q) {
        println "Whelk ${this.class.name} received query ${q}"
    }

    def ingest(MyDocument d, boolean bulk = false) {
        storage.store(d)
        return d.identifier
    }

    def retrieve(identifier) {
        identifier = new URI(identifier)
        storage.retrieve(identifier)
    }
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
        def storage = new DiskStorage(whelk_storage)
        def whelk = new Whelk(storage)

        def api = new API()
        api.addWhelk(whelk)

        /*
        api.query('Fragile Things')
        */

        if (args.length > 1 && args[0] == 'retrieve') {
            println "Loading file ${args[1]}"
            def ldoc = whelk.retrieve(args[1])
            println "File:"
            print new String(ldoc.data)
        }
        else if (args.length > 0) {
            def file = new File(args[0])
            println "Storing file ${file}"
            println "${file.text}"
            def doc = new MyDocument(file.text)
            def identifier = whelk.ingest(doc)
            println "Stored document with identifier ${identifier}"
        }
    }
}
