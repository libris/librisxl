package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.BasicPlugin
import se.kb.libris.whelks.plugin.Plugin

import gov.loc.repository.pairtree.Pairtree

@Log
class DiskStorage extends BasicPlugin implements Storage {
    String storageDir = "./storage"
    String versionsStorageDir = null
    boolean enabled = true

    String id = "diskstorage"
    String docFolder = "_"
    List contentTypes
    boolean isVersioning

    int PATH_CHUNKS=4

    static final String METAFILE_EXTENSION = ".entry"
    static final String DATAFILE_EXTENSION = ".data"
    static final String MAIN_STORAGE_DIR = "main"
    static final String VERSIONS_STORAGE_DIR = "versions"

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
        this.isVersioning = settings.get('versioning', false)

        log.info("Starting DiskStorage with storageDir $storageDir")
    }

    void init(String stName) {
        if (isVersioning) {
            this.versionsStorageDir = this.storageDir + "/" + stName + "/" + VERSIONS_STORAGE_DIR
        }
        this.storageDir = this.storageDir + "/" + stName + "/" + MAIN_STORAGE_DIR
    }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    OutputStream getOutputStreamFor(Document doc) {
        File file = new File(buildPath(doc.identifier))
        return file.newOutputStream()
    }

    @Override
    @groovy.transform.CompileStatic
    boolean store(Document doc, boolean checkExisting = true, int withVersion = 0) {
        if (doc && (handlesContent(doc.contentType) || doc.entry.deleted)) {
            if (this.isVersioning && checkExisting) {
                doc = checkAndUpdateExisting(doc)
            }
            String filePath = buildPath(doc.identifier, true, withVersion) + "/" + getBaseFilename(doc.identifier)
            File sourcefile = new File(filePath + DATAFILE_EXTENSION)
            File metafile = new File(filePath + METAFILE_EXTENSION)
            try {
                log.trace("Saving file with path ${sourcefile.path}")
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

    Document checkAndUpdateExisting(Document doc) {
        log.trace("checking for existingdoc with identifier ${doc.identifier}")
        Document existingDocument = get(doc.identifier)
        log.trace("found: $existingDocument")
        int version = 1
        if (existingDocument) {
            if (existingDocument?.entry?.checksum == doc.entry?.checksum) {
                throw new DocumentException(DocumentException.IDENTICAL_DOCUMENT, "Identical document already stored.")
            }
            version = existingDocument.version + 1
            Map versions = existingDocument.entry.versions ?: [:]
            String lastVersion = existingDocument.version as String
            versions[lastVersion] = ["timestamp" : existingDocument.timestamp]
            if (existingDocument?.entry?.deleted) {
                versions.get(lastVersion).put("deleted", true)
            } else {
                versions.get(lastVersion).put("checksum",existingDocument.entry.checksum)
            }
            doc.entry.put("versions", versions)
            store(existingDocument, false, existingDocument.version)
        }
        log.trace("Setting document version: $version")
        doc.withVersion(version)
        return doc
    }

    @groovy.transform.CompileStatic
    String getBaseFilename(String identifier) {
        identifier.substring(identifier.lastIndexOf("/")+1)
    }

    @groovy.transform.CompileStatic
    Document get(String uri, String version=null) {
        return get(new URI(uri), version)
    }

    @Override
    @groovy.transform.CompileStatic
    Document get(URI uri, String version = null) {
        log.trace("Received GET request for ${uri.toString()} with version $version")
        String filePath = buildPath(uri.toString(), false, (version ? version as int : 0)) + "/" + getBaseFilename(uri.toString())
        try {
            log.trace("filePath: $filePath")
            File metafile = new File(filePath + METAFILE_EXTENSION)
            File sourcefile = new File(filePath + DATAFILE_EXTENSION)
            def document = new Document(sourcefile, metafile)
            return document
        } catch (FileNotFoundException fnfe) {
            log.trace("Files on $filePath not found.")
            if (version) {
                log.debug("Trying to see if requested version is actually current version.")
                def document = get(uri)
                if (document && document.version == version as int) {
                    log.debug("Why, yes it was!")
                    return document
                } else {
                    log.debug("Nah, it wasn't")
                }
            }
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
        for (doc in docs) {
            store(doc)
        }
    }

    @Override
    void delete(URI uri) {
        if (isVersioning) {
            store(createTombstone(uri))
        } else {
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
    }

    private Document createTombstone(uri) {
        def tombstone = new Document().withIdentifier(uri).withData("DELETED ENTRY")
        tombstone.entry['deleted'] = true
        return tombstone
    }

    String buildPath(String id, boolean createDirectories, int version = 0) {
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
    String buildPath(String id, boolean createDirectories, int version = 0) {
        int pos = id.lastIndexOf("/")
        String path
        String baseDir = (version > 0 ? this.versionsStorageDir : this.storageDir)
        String encasingDir = (version > 0 ? version as String : null)
        if (pos != -1) {
            path = pairtree.mapToPPath(baseDir + id.substring(0, pos), id.substring(pos+1), encasingDir)
        } else {
            path = pairtree.mapToPPath(baseDir, id, encasingDir)
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
    String buildPath(String id, boolean createDirectories, int version = 0) {
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
                    def document = new Document(new File(currentFile.parent + "/" + fileBaseName(currentFile.name) + DiskStorage.DATAFILE_EXTENSION), currentFile)
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
