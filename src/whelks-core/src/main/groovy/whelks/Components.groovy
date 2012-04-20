package se.kb.libris.conch.component

interface Component {
    def add(Document d)
}

interface Storage extends Component {}

interface Index extends Component {
    def index(Document d, def indexName, def type)
}

class DiskStorage implements Component { 
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
