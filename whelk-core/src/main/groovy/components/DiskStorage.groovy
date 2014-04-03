package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.BasicPlugin
import se.kb.libris.whelks.plugin.Plugin

import gov.loc.repository.pairtree.Pairtree

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.*

@Log
class PairtreeDiskStorage extends BasicPlugin implements Storage {
    String baseStorageDir = "./storage"
    String storageDir = null
    String versionsStorageDir = null
    String baseStorageSuffix = null
    boolean enabled = true

    String id = "diskstorage"
    String docFolder = "_"
    List contentTypes
    boolean versioning

    int PATH_CHUNKS=4

    final static Pairtree pairtree = new Pairtree()

    static final String ENTRY_FILE_NAME = "entry.json"
    static final String DATA_FILE_NAME = "content.data"
    static final String DATAFILE_EXTENSION = ".data"
    static final String MAIN_STORAGE_DIR = "main"
    static final String VERSIONS_STORAGE_DIR = "versions"
    static final String FILE_NAME_KEY = "dataFileName"

    static final String VERSION_DIR_PREFIX = "version_"

    static final Map FILE_EXTENSIONS = [
        "application/json" : ".json",
        "application/ld+json" : ".jsonld",
        "application/x-marc-json" : ".json",
        "application/xml" : ".xml",
        "text/xml" : ".xml"
    ]

    // TODO: Add document counter

    PairtreeDiskStorage(Map settings) {
        StringBuilder dn = new StringBuilder(settings['storageDir'])
        while (dn[dn.length()-1] == '/') {
            dn.deleteCharAt(dn.length()-1)
        }
        this.baseStorageDir = dn.toString()
        this.contentTypes = settings.get('contentTypes', null)
        this.versioning = settings.get('versioning', false)
        this.baseStorageSuffix = settings.get('baseStorageSuffix', null)
    }

    void init(String stName) {
        if (!this.baseStorageSuffix) {
            this.baseStorageSuffix = this.id
        }
        if (versioning) {
            this.versionsStorageDir = this.baseStorageDir + "/" + stName + "_" + this.baseStorageSuffix + "/" + VERSIONS_STORAGE_DIR
        }
        this.storageDir = this.baseStorageDir + "/" + stName + "_" + this.baseStorageSuffix + "/" + MAIN_STORAGE_DIR
        log.info("Starting DiskStorage with storageDir $storageDir ${(versioning ? "and versions in $versionsStorageDir" : "")}")
    }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    OutputStream getOutputStreamFor(Document doc) {
        File file = new File(buildPath(doc.identifier))
        return file.newOutputStream()
    }

    @Override
    @groovy.transform.CompileStatic
    boolean store(Document doc) {
        if (doc && (handlesContent(doc.contentType) || doc.entry.deleted)) {
            if (this.versioning) {
                doc = checkAndUpdateExisting(doc)
            }
            String filePath = buildPath(doc.identifier)
            return writeDocumentToDisk(doc, filePath, getBaseFilename(doc.identifier))
        }
        return false
    }

    @groovy.transform.CompileStatic
    private boolean writeDocumentToDisk(Document doc, String filePath, String fileName) {
        String extension = FILE_EXTENSIONS.get(doc.contentType, DATAFILE_EXTENSION)
        log.trace("Using extension: $extension")
        String sourcefilePath = filePath + "/" + fileName + extension
        File sourcefile = new File(sourcefilePath)
        File metafile = new File(filePath + "/" + ENTRY_FILE_NAME)
        try {
            log.trace("Saving file with path ${sourcefile.path}")
            FileUtils.writeByteArrayToFile(sourcefile, doc.data)
            log.trace("Setting entry in document meta")
            doc.getEntry().put(FILE_NAME_KEY, sourcefilePath)
            log.trace("Saving file with path ${metafile.path}")
            FileUtils.write(metafile, doc.metadataAsJson, "utf-8")
            return true
        } catch (IOException ioe) {
            log.error("Write failed: ${ioe.message}", ioe)
            throw ioe
        }
        return false
    }

    Document checkAndUpdateExisting(Document doc) {
        log.trace("checking for existingdoc with identifier ${doc.identifier}")
        Document existingDocument = get(doc.identifier)
        log.trace("found: $existingDocument")
        int version = 1
        if (existingDocument) {
            if (existingDocument.entry?.checksum == doc.entry?.checksum) {
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
            writeDocumentToDisk(existingDocument, buildPath(existingDocument.identifier, existingDocument.version), getBaseFilename(existingDocument.identifier))
        }
        log.trace("Setting document version: $version")
        return doc.withVersion(version)
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
        String filePath = buildPath(uri.toString(), (version ? version as int : 0))
        String fileName =  getBaseFilename(uri.toString())
        try {
            log.trace("filePath: $filePath")
            File metafile = new File(filePath + "/" + ENTRY_FILE_NAME)
            def document = new Document(FileUtils.readFileToString(metafile, "utf-8"))
            File sourcefile = new File(filePath + "/" + fileName + FILE_EXTENSIONS.get(document.contentType, DATAFILE_EXTENSION))
            return document.withData(FileUtils.readFileToByteArray(sourcefile))
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
    @groovy.transform.CompileStatic
    Iterable<Document> getAll(String dataset = null, Date since = null) {
        String baseDir = (dataset != null ? new File(this.storageDir + "/" + dataset) : new File(this.storageDir))
        final Iterator<File> entryIterator = FileUtils.iterateFiles(new File(baseDir), new NameFileFilter(ENTRY_FILE_NAME), HiddenFileFilter.VISIBLE)

        return new Iterable<Document>() {
            @Override
            Iterator<Document> iterator() {
                return new Iterator<Document>() {
                    public boolean hasNext() { return entryIterator.hasNext(); }
                    public Document next() {
                        while (entryIterator.hasNext()) {
                            File entryFile = entryIterator.next()
                            Document document = new Document(FileUtils.readFileToString(entryFile, "utf-8"))
                            return document.withData(FileUtils.readFileToByteArray(new File(document.getEntry().get(PairtreeDiskStorage.FILE_NAME_KEY))))
                        }
                        throw new NoSuchElementException();
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                };
            }
        }
    }

    @Override
    void delete(URI uri) {
        if (versioning) {
            store(createTombstone(uri))
        } else {
            try {
                def fn = buildPath(uri.toString())
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

    @groovy.transform.CompileStatic
    String buildPath(String id, int version = 0) {
        int pos = id.lastIndexOf("/")
        String path
        String baseDir = (version > 0 ? this.versionsStorageDir : this.storageDir)
        String encasingDir = (version > 0 ? VERSION_DIR_PREFIX + version : null)
        if (pos != -1) {
            path = pairtree.mapToPPath(baseDir + id.substring(0, pos), id.substring(pos+1), encasingDir)
        } else {
            path = pairtree.mapToPPath(baseDir, id, encasingDir)
        }
        return path
    }

    private Document createTombstone(uri) {
        def tombstone = new Document().withIdentifier(uri).withData("DELETED ENTRY")
        tombstone.entry['deleted'] = true
        return tombstone
    }


    @Override
    boolean handlesContent(String ctype) {
        return (ctype == "*/*" || !this.contentTypes || this.contentTypes.contains(ctype))
    }
}


@Deprecated
class DiskStorage extends PairtreeDiskStorage {


    DiskStorage(Map settings) {
       super(settings)
    }


    @Override
    String buildPath(String id, int version = 0) {
        def path = this.storageDir + "/" + id.substring(0, id.lastIndexOf("/"))
        def basename = id.substring(id.toString().lastIndexOf("/")+1)

        for (int i=0; i*PATH_CHUNKS+PATH_CHUNKS < basename.length(); i++) {
            path = path + "/" + basename[i*PATH_CHUNKS .. i*PATH_CHUNKS+PATH_CHUNKS-1].replaceAll(/[\.]/, "")
        }

        if (this.docFolder) {
            path = path + "/" + this.docFolder + "/" + basename
        }
        return path.replaceAll(/\/+/, "/") //+ "/" + basename
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
