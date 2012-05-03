package se.kb.libris.conch.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.conch.*
import se.kb.libris.conch.data.*
import se.kb.libris.conch.plugin.*


interface Component extends Plugin {
    def add(Document d)
    def retrieve(URI u)
}

interface Storage extends Component {
}

interface Index extends Component {
    def find(def query, def index)
}

@Log
class DiskStorage implements Storage {
    def storageDir = "./storage/"
    Whelk whelk

    DiskStorage() {
        def env = System.getenv()
        def whelk_storage = (env["PROJECT_HOME"] ? env["PROJECT_HOME"] : System.getProperty("user.home")) + "/whelk_storage"
        setStorageDir(whelk_storage)
    }

    DiskStorage(def directoryName) {
        setStorageDir(directoryName)
    }

    def setWhelk(Whelk w) { this.whelk = w }

    def init() {
        log.debug "Making sure directory $storageDir exists ..."
        new File(storageDir).mkdirs()
    }

    def setStorageDir(String directoryName) { 
        storageDir = directoryName 
        init()
    }

    def add(Document d) {
        def filename = (d.identifier ? d.identifier.toString() : _create_filename())
        log.debug "${this.class.name} storing file $filename in $storageDir"
        def fullpath = storageDir + "/" + filename
        def path = fullpath.substring(0, fullpath.lastIndexOf("/"))
        log.debug "PATH: $path"
        new File(path).mkdirs()
        File file = new File("$storageDir/$filename")
        file.write(new String(d.data))
        d.identifier = new URI(filename)
    }

    Document retrieve(URI u) {
        def s 
        def filename = u.toString()
        File f = new File("$storageDir/$filename")
        try {
            return new MyDocument(filename).withData(f.text.getBytes())
        } catch (FileNotFoundException fnfe) {
            return null
        }
    }

    def _create_filename() {
        def pool = ['a'..'z','A'..'Z',0..9,'_'].flatten()
        Random rand = new Random(System.currentTimeMillis())

        def passChars = (0..10).collect { pool[rand.nextInt(pool.size())] }
        passChars.join()
    }
}
