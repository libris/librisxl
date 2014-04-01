package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.plugin.BasicPlugin
import se.kb.libris.whelks.plugin.Plugin

import gov.loc.repository.pairtree.Pairtree

@Log
class DiskStorage extends BasicPlugin implements Storage {
    String storageDir = "./storage"
    boolean enabled = true

    String id = "diskstorage"
    String docFolder = "_"
    List contentTypes

    int PATH_CHUNKS=4

    static final String METAFILE_EXTENSION = ".entry"
    static final String DATAFILE_EXTENSION = ".data"

    /*
    static final FILE_EXTENSIONS = [
        "application/json" : ".json",
        "application/ld+json" : ".jsonld",
        "application/x-marc-json" : ".json",
        "application/xml" : ".xml",
        "text/xml" : ".xml"
    ]
    */

    DiskStorage(Map settings) {
        StringBuilder dn = new StringBuilder(settings['storageDir'])
        while (dn[dn.length()-1] == '/') {
            dn.deleteCharAt(dn.length()-1)
        }
        this.storageDir = dn.toString()
        this.contentTypes = settings.get('contentTypes', null)

        log.info("Starting DiskStorage with storageDir $storageDir")
    }

    void init(String stName) {
        this.storageDir = this.storageDir + "/" + stName
    }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    OutputStream getOutputStreamFor(Document doc) {
        File file = new File(buildPath(doc.identifier))
        return file.newOutputStream()
    }

    @Override
    boolean store(Document doc) {
        if (doc && handlesContent(doc.contentType)) {
            String filePath = buildPath(doc.identifier, true) + "/" + getBaseFilename(doc.identifier)
            File sourcefile = new File(filePath + DATAFILE_EXTENSION)
            File metafile = new File(filePath + METAFILE_EXTENSION)
            try {
                sourcefile.write(doc.dataAsString)
                metafile.write(doc.metadataAsJson)
            } catch (IOException ioe) {
                log.error("Write failed: ${ioe.message}", ioe)
                throw ioe
            }
            return true
        }
        return false
    }

    String getBaseFilename(String identifier) {
        identifier.substring(identifier.lastIndexOf("/")+1)
    }

    Document get(String uri, String version=null) {
        return get(new URI(uri), version)
    }

    @Override
    Document get(URI uri, String version = null) {
        if (version) {
            throw new WhelkRuntimeException("Storage does not support versioning.")
        }
        log.trace("Loading from ${this.getClass()}")
        try {
            String filePath = buildPath(uri.toString(), false) + "/" + getBaseFilename(uri.toString())
            log.debug("buildPath: " + buildPath(uri.toString(), false))
            log.debug("basename: " + getBaseFilename(uri.toString()))
            File metafile = new File(filePath + METAFILE_EXTENSION)
            File sourcefile = new File(filePath + DATAFILE_EXTENSION)
            def document = new Document(sourcefile, metafile)
            log.debug("Loading document from disk.")
            return document
        } catch (FileNotFoundException fnfe) {
            log.trace("File $sourcefile or $metafile not found.")
            return null
        }
    }

    @Override
    Iterable<Document> getAll(String dataset = null, Date since = null) {
        def baseDir = (dataset != null ? new File(this.storageDir + "/" + dataset) : new File(this.storageDir))
        log.debug("Basedir: $baseDir")
        return new DiskDocumentIterable<Document>(baseDir)
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
            def fn = buildPath(uri.toString(), false)
            log.debug("Deleting $fn")
            if (!new File(fn).deleteDir()) {
                log.error("Failed to delete $uri")
                throw new WhelkRuntimeException("" + this.getClass().getName() + " failed to delete $uri")
            }
        } catch (Exception e) {
            throw new WhelkRuntimeException(e)
        }
    }

    String buildPath(String id, boolean createDirectories) {
        def path = this.storageDir + "/" + id.substring(0, id.lastIndexOf("/"))
        def basename = id.substring(id.toString().lastIndexOf("/")+1)

        for (int i=0; i*PATH_CHUNKS+PATH_CHUNKS < basename.length(); i++) {
            path = path + "/" + basename[i*PATH_CHUNKS .. i*PATH_CHUNKS+PATH_CHUNKS-1].replaceAll(/[\.]/, "")
        }

        if (this.docFolder) {
            path = path + "/" + this.docFolder + "/" + basename
        }
        /* Disable this
        if (createDirectories) {
            new File(path).mkdirs()
        }
        */
        return path.replaceAll(/\/+/, "/") //+ "/" + basename
    }

    @Override
    boolean handlesContent(String ctype) {
        return (ctype == "*/*" || !this.contentTypes || this.contentTypes.contains(ctype))
    }
}


@Log
class PairtreeDiskStorage extends DiskStorage {

    final static Pairtree pairtree = new Pairtree()

    PairtreeDiskStorage(Map settings) {
       super(settings)
    }

    @Override
    @groovy.transform.CompileStatic
    String buildPath(String id, boolean createDirectories) {
        int pos = id.lastIndexOf("/")
        String path
        if (pos != -1) {
            path = pairtree.mapToPPath(this.storageDir + id.substring(0, pos), id.substring(pos+1), null)
        } else {
            path = pairtree.mapToPPath(this.storageDir, id, null)
        }
        if (createDirectories) {
            new File(path).mkdirs()
        }
        return path
    }

}

@Log
class FlatDiskStorage extends DiskStorage {


    FlatDiskStorage(Map settings) {
        super(settings)
    }


    @Override
    String buildPath(String id, boolean createDirectories) {
        def path = (this.storageDir + "/" + new URI(id).path).replaceAll(/\/+/, "/")
        if (createDirectories) {
            new File(path).mkdirs()
        }
        return path
    }
}

/*
 * Utility classes.
 */
@Log
class DiskDocumentIterable implements Iterable<Document> {
    File baseDirectory
    DiskDocumentIterable(File bd) {
        this.baseDirectory = bd
    }

    Iterator<Document> iterator() {
        return new DiskDocumentIterator(this.baseDirectory)
    }


    class DiskDocumentIterator implements Iterator<Document> {

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

        private String fileBaseName(String filename) {
            if (filename.lastIndexOf(".") != -1) {
                return filename.substring(0, filename.lastIndexOf("."))
            }
            return filename
        }


        private void populateResults() {

            while (!fileStack.isEmpty() && resultQueue.isEmpty()) {
                File currentFile = fileStack.pop();

                if (currentFile.isFile() && currentFile.length() > 0 && currentFile.name.endsWith(DiskStorage.METAFILE_EXTENSION)) {
                    /*

                    def document = new Document(currentFile.text)
                    def fileBaseName = currentFile.parent + "/" + currentFile.name.lastIndexOf('.').with {it != -1 ? currentFile.name[0..<it] : currentFile.name}
                    def sourcefile = 
                    */
                    def document = new Document(new File(currentFile.parent + "/" + fileBaseName(currentFile.name) + DiskStorage.DATAFILE_EXTENSION), currentFile)
                    //document.data = sourcefile.readBytes()
                    resultQueue.offer(document)
                }

                if (currentFile.isDirectory()) {
                    fileStack.addAll(Arrays.asList(currentFile.listFiles()))
                }
            }
        }

        public void remove() {}

    }
}
