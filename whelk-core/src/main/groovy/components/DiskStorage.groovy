package se.kb.libris.whelks.component

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import groovy.transform.Synchronized

import java.util.concurrent.*

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.BasicPlugin
import se.kb.libris.whelks.plugin.Plugin

import se.kb.libris.conch.Tools

import gov.loc.repository.pairtree.Pairtree

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.*

import com.google.common.io.Files



@Deprecated
class DiskStorage extends PairtreeDiskStorage implements Storage {

    String docFolder = "_"
    int PATH_CHUNKS=4

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
class FileWalker implements Iterator<Document> {
    final BlockingQueue<Document> bq
    FileWalker(final File fileStart, final int size) throws Exception {
        bq = new ArrayBlockingQueue<Document>(size)
        Thread thread = new Thread(new Runnable() {
            static final Logger log = LoggerFactory.getLogger("se.kb.libris.whelks.component.PairtreeDiskStorage")
            public void run() {
                try {
                    Files.walkFileTree(fileStart.toPath(), new FileVisitor<Path>() {
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                            return FileVisitResult.CONTINUE
                        }
                        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                            File file = path.toFile()
                            if (file.name != PairtreeDiskStorage.ENTRY_FILE_NAME) {
                                return FileVisitResult.CONTINUE
                            }
                            Document document = new Document(FileUtils.readFileToString(file, "utf-8"))
                            try {
                                document.withData(FileUtils.readFileToByteArray(new File(file.getParentFile(), document.getEntry().get(PairtreeDiskStorage.FILE_NAME_KEY))))
                            } catch (FileNotFoundException fnfe) {
                                log.trace("File not found using ${document.getEntry().get(PairtreeDiskStorage.FILE_NAME_KEY)} as filename. Will try to use it as path.")
                                document.withData(FileUtils.readFileToByteArray(new File(document.getEntry().get(PairtreeDiskStorage.FILE_NAME_KEY))))
                            } catch (InterruptedException e) {
                                e.printStackTrace()
                            }
                            log.trace("Offering document ${document.identifier} to queue.")
                            super.bq.offer(document, 4242, TimeUnit.HOURS)
                            return FileVisitResult.CONTINUE
                        }
                        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                            return FileVisitResult.CONTINUE
                        }
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                            return FileVisitResult.CONTINUE
                        }
                    })
                } catch (IOException e) {
                    e.printStackTrace()
                }
            }
        })
        thread.setDaemon(true)
        thread.start()
        thread.join(200)
    }
    public boolean hasNext() {
        boolean hasNext = false
        long dropDeadMS = System.currentTimeMillis() + 2000
        while (System.currentTimeMillis() < dropDeadMS) {
            if (bq.peek() != null) {
                hasNext = true
                break
            }
            try {
                Thread.sleep(1)
            } catch (InterruptedException e) {
                e.printStackTrace()
            }
        }
        return hasNext
    }
    public Document next() {
        Document document = null
        try {
            document = bq.take()
        } catch (InterruptedException e) {
            e.printStackTrace()
        }
        return document
    }
    public void remove() {
        throw new UnsupportedOperationException()
    }

}
*/
