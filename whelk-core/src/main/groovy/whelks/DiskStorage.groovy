package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import java.text.*

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
    Map<URI, Long> inventory

    int PATH_CHUNKS=4
    final String INVENTORY_FILE = "inventory.data"

    static final String METAFILE_EXTENSION = ".entry"

    static final FILE_EXTENSIONS = [
        "application/json" : ".json",
        "application/xml" : ".xml",
        "text/xml" : ".xml"
    ]

    DiskStorage(String directoryName) {
        StringBuilder dn = new StringBuilder(directoryName)
        while (dn[dn.length()-1] == '/') {
            dn.deleteCharAt(dn.length()-1)
        }
        this.storageDir = dn.toString()

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
    void store(Document doc, String whelkPrefix, boolean saveInventory = true) {
        File sourcefile = new File(buildPath(doc.identifier, true) + "/" + getBaseFilename(doc.identifier) + (FILE_EXTENSIONS[doc.contentType] ?: ""))
        File metafile = new File(buildPath(doc.identifier, true) + "/"+ getBaseFilename(doc.identifier) + METAFILE_EXTENSION)
        sourcefile.write(doc.dataAsString)
        metafile.write(doc.toJson())
    }

    String getBaseFilename(uri) {
        uri.toString().substring(uri.toString().lastIndexOf("/")+1)
    }

    @Override
    Document get(URI uri, String whelkPrefix) {
        File metafile = new File(buildPath(uri, false)+ "/" + getBaseFilename(uri) + METAFILE_EXTENSION)
        File sourcefile
        try {
            log.debug("buildPath: " + buildPath(uri, false))
            log.debug("basename: " + getBaseFilename(uri))
            def document = new BasicDocument(metafile.text)
            log.debug("ext: " + FILE_EXTENSIONS[document.contentType])
            sourcefile = new File(buildPath(uri, false) + "/" + getBaseFilename(uri) + (FILE_EXTENSIONS[document.contentType] ?: ""))
            document.data = sourcefile.readBytes()
            log.debug("Loading document from disk.")
            return document
        } catch (FileNotFoundException fnfe) {
            log.warn("File $sourcefile or $metafile not found.")
            return null
        }
    }
    /*
    @Override
    Document get(URI uri, String whelkPrefix) {
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
    void store(Document doc, String whelkPrefix, boolean saveInventory = true) {
        File file = new File(buildPath(doc.identifier, true))
        file.write(doc.toJson())
    }
    */

    @Override
    Iterable<Document> getAll(String whelkPrefix) {
        def baseDir = new File(this.storageDir+"/"+ whelkPrefix)
        return new DiskDocumentIterable(baseDir)
    }

    @Override
    void store(Iterable<Document> docs, String whelkPrefix) {
        docs.each {
            store(it, whelkPrefix, false)
        }
    }

    @Override
    void delete(URI uri, String whelkPrefix) {
        try {
            if (!new File(buildPath(uri, false)).deleteDir()) {
                log.error("Failed to delete $uri")
                throw new WhelkRuntimeException("" + this.getClass().getName() + " failed to delete $uri")
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
            path = path + "/" + this.docFolder + "/" + basename
        }
        if (createDirectories) {
            new File(path).mkdirs()
        }
        return path.replaceAll(/\/+/, "/") //+ "/" + basename
    }
}

@Log
class FlatDiskStorage extends DiskStorage {


    FlatDiskStorage(String directoryName) {
        super(directoryName)
    }


    @Override
    String buildPath(URI id, boolean createDirectories) {
        def path = (this.storageDir + "/" + id.path).replaceAll(/\/+/, "/")
        if (createDirectories) {
            new File(path).mkdirs()
        }
        return path
    }
}

/*
 * Utility classes.
 */
class DiskDocumentIterable implements Iterable<Document> {
    File baseDirectory
    DiskDocumentIterable(File bd) {
        this.baseDirectory = bd

    }

    Iterator<Document> iterator() {
        return new DiskDocumentIterator(this.baseDirectory)
    }


    class DiskDocumentIterator<Document> implements Iterator {

        private LinkedList<File> fileStack = new LinkedList<File>()
        private Queue<Document> resultQueue = new LinkedList<Document>()

        DiskDocumentIterator(File startDirectory) {
            if (startDirectory && startDirectory.exists()) {
                fileStack.addAll(startDirectory.listFiles())
            }
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
                    def document
                    if (currentFile.name.endsWith(DiskStorage.METAFILE_EXTENSION)) {
                        document = new BasicDocument(currentFile.text)
                        def fileBaseName = currentFile.parent + "/" + currentFile.name.lastIndexOf('.').with {it != -1 ? currentFile.name[0..<it] : currentFile.name}
                        def sourcefile = new File(fileBaseName + (DiskStorage.FILE_EXTENSIONS[document.contentType] ?: ""))
                        document.data = sourcefile.readBytes()
                    }
                    if (document) {
                        resultQueue.offer(document)
                    }
                }

                if (currentFile.isDirectory()) {
                    fileStack.addAll(Arrays.asList(currentFile.listFiles()))
                }
            }
        }

        public void remove() {}

    }
}
