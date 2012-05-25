package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class DiskStorage implements Storage {
    def storageDir = "./storage/"
    Whelk whelk
    boolean enabled = true

    String id = "diskstorage"

    DiskStorage() {
        def env = System.getenv()
        def whelk_storage = (env["PROJECT_HOME"] ? env["PROJECT_HOME"] : System.getProperty("user.home")) + "/whelk_storage"
        setStorageDir(whelk_storage)
    }

    DiskStorage(def directoryName) {
        setStorageDir(directoryName)
    }

    def void setWhelk(se.kb.libris.whelks.Whelk w) { this.whelk = w }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    @Override
    OutputStream getOutputStreamFor(Document doc) {
        def filename = doc.identifier.toString()
        log.debug "${this.class.name} storing file $filename in $storageDir"
        def fullpath = storageDir + "/" + filename
        def path = fullpath.substring(0, fullpath.lastIndexOf("/"))
        log.debug "PATH: $path"
        new File(path).mkdirs()
        File file = new File("$storageDir/$filename")
        return file.newOutputStream()
        //file.write(new String(d.data))
    }

    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document get(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    void store(Document doc) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    void delete(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    def init() {
        log.debug "Making sure directory $storageDir exists ..."
        new File(storageDir).mkdirs()
    }

    def setStorageDir(String directoryName) { 
        storageDir = directoryName 
        init()
    }

    def deprecated_add(Document d) {
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

    Document retrieve(URI u, raw=false) {
        def s 
        def filename = u.toString()
        File f = new File("$storageDir/$filename")
        try {
            return this.whelk.createDocument().withIdentifier(u).withContentType("content/type").withData(f.text.getBytes())
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
