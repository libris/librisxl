package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

@Log
class DiskStorage extends BasicPlugin implements Storage {
    def storageDir = "./storage"
    boolean enabled = true

    String id = "diskstorage"
    String docFolder = "_"

    int PATH_CHUNKS=4

    DiskStorage(String directoryName) {
        StringBuilder dn = new StringBuilder(directoryName)
        while (dn[dn.length()-1] == '/') {
            dn.deleteCharAt(dn.length()-1)
        }
        this.storageDir = dn.toString() // + "/" + whelkPrefix
        log.info("Starting DiskStorage with storageDir $storageDir")
    }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    OutputStream getOutputStreamFor(Document doc) {
        File file = new File(buildPath(doc.identifier))
        return file.newOutputStream()
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    @Override
    Document get(URI uri) {
        File f = new File(buildPath(uri, false))
        try {
            def document = new BasicDocument(f.text)
            log.debug("Loading document from disk.")
            return document
        } catch (FileNotFoundException fnfe) {
            return null
        }
    }

    @Override
    Iterable<Document> getAll() {
        def baseDir = new File(this.storageDir)
        return new DiskDocumentIterable(baseDir)
    }

    void index(Document doc) {
        def filename = doc.identifier.toString() + ".index"
            log.debug "${this.class.name} storing file $filename in $storageDir"
            def fullpath = storageDir + "/" + filename
            def path = fullpath.substring(0, fullpath.lastIndexOf("/"))
            log.debug "PATH: $path"
            new File(path).mkdirs()
            File file = new File("$storageDir/$filename")
            file.write(doc.dataAsString)
    }
    public void index(Iterable<Document> d) {
        for (doc in d) {
            index(doc)
        }
    }

    @Override
    void store(Document doc) {
        File file = new File(buildPath(doc.identifier, true))
            file.write(doc.toJson())
    }

    @Override
    void store(Iterable<Document> docs) {
        docs.each {
            store(it)
        }
    }

    @Override
    void delete(URI uri) {
        try {
            if (!new File(buildPath(uri, false)).delete()) {
                log.error("Failed to delete $uri")
            }
        } catch (Exception e) {
            throw new WhelkRuntimeException(e)
        }
    }

    String buildPath(URI id, boolean createDirectories) {
        def path = this.storageDir + "/" + id.toString().substring(0, id.toString().lastIndexOf("/"))
        def basename = id.toString().substring(id.toString().lastIndexOf("/")+1)

        for (int i=0; i*PATH_CHUNKS+PATH_CHUNKS < basename.length(); i++) {
            path = path + "/" + basename[i*PATH_CHUNKS .. i*PATH_CHUNKS+PATH_CHUNKS-1].replaceAll(/[\.]/, "")
        }

        if (this.docFolder) {
            path = path + "/" + this.docFolder
        }
        if (createDirectories) {
            new File(path).mkdirs()
        }
        return path.replaceAll(/\/+/, "/") + "/" + basename
    }

    class DiskDocumentIterable implements Iterable<Document> {
        File baseDirectory
        DiskDocumentIterable(File bd) {
            this.baseDirectory = bd

        }

        Iterator<Document> iterator() {
            return new DiskDocumentIterator(this.baseDirectory)
        }
    }

    class DiskDocumentIterator<Document> implements Iterator {

        private LinkedList<File> fileStack = new LinkedList<File>()
        private Queue<Document> resultQueue = new LinkedList<Document>()

        DiskDocumentIterator(File startDirectory) {
            fileStack.addAll(startDirectory.listFiles())
        }

        public boolean hasNext() {
            if (resultQueue.isEmpty()) {
                populateResults();
            }
            return !resultQueue.isEmpty();
        }

        public Document next() {
            if (resultQueue.isEmpty()) {
                populateResults();
            }
            return resultQueue.poll();
        }


        private void populateResults() {

            while (!fileStack.isEmpty() && resultQueue.isEmpty()) {
                File currentFile = fileStack.pop();

                if (currentFile.isFile() && currentFile.length() > 0) {
                    def d = new BasicDocument(currentFile.text)
                    resultQueue.offer(d)
                }

                if (currentFile.isDirectory()) {
                    fileStack.addAll(Arrays.asList(currentFile.listFiles()))
                }
            }
        }

        public void remove() {}

    }
}
