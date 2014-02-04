package se.kb.libris.whelks.importers

import groovy.xml.StreamingMarkupBuilder
import groovy.util.logging.Slf4j as Log
import groovy.time.*

import java.text.*

import java.util.concurrent.*

import javax.xml.stream.*
import javax.xml.transform.Transformer
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stax.StAXSource
import javax.xml.transform.stream.StreamResult

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import se.kb.libris.conch.converter.MarcJSONConverter

import static se.kb.libris.conch.Tools.*

@Log
class DumpImporter {

    Whelk whelk
    int nrImported = 0
    int nrDeleted = 0
    boolean picky
    final int BATCH_SIZE = 1000
    String origin

    File failedLog
    File exceptionLog

    ExecutorService queue

    def progressSpinner = ['/','-','\\','|']
    int progressSpinnerState = 0

    DumpImporter(Whelk toWhelk, String origin, boolean picky = true) {
        failedLog = new File("failed_ids.log")
        exceptionLog = new File("exceptions.log")
        this.whelk = toWhelk
        this.picky = picky
        this.origin = origin
    }

    int doImportFromFile(File file) {
        log.info("Loading dump from $file. Picky mode: $picky")
        XMLInputFactory xif = XMLInputFactory.newInstance()
        XMLStreamReader xsr = xif.createXMLStreamReader(new FileReader(file))
        return performImport(xsr)
    }

    int doImportFromURL(URL url) {
        log.info("Loading dump from ${url.toString()}. Picky mode: $picky")
        XMLInputFactory xif = XMLInputFactory.newInstance()
        //XMLStreamReader xsr = xif.createXMLStreamReader(new URL(urlString).newInputStream())
        XMLStreamReader xsr = xif.createXMLStreamReader(url.newInputStream())
        return performImport(xsr)
    }

    int performImport(XMLStreamReader xsr) {
        queue = Executors.newSingleThreadExecutor()
        xsr.nextTag(); // Advance to statements element
        def documents = []
        Transformer optimusPrime = TransformerFactory.newInstance().newTransformer()
        while(xsr.nextTag() == XMLStreamConstants.START_ELEMENT) {
            //Date loadStartTime = new Date()
            long loadStartTime = System.nanoTime()
            Writer outWriter = new StringWriter()
            Document doc = null
            try {
                optimusPrime.transform(new StAXSource(xsr), new StreamResult(outWriter))
                String xmlString = normalizeString(outWriter.toString())
                doc = buildDocument(xmlString)
                doc = whelk.sanityCheck(doc)
            } catch (javax.xml.stream.XMLStreamException xse) {
                log.error("Skipping document, error in stream: ${xse.message}")
            }

            if (doc) {
                documents << doc
                printSpinner("Running dumpimport. $nrImported documents imported sofar.", nrImported)
                if (++nrImported % BATCH_SIZE == 0) {
                    addDocuments(documents)
                    documents = []
                    float elapsedTime = ((System.nanoTime()-loadStartTime)/1000000000)
                    log.debug("imported: $nrImported time: $elapsedTime velocity: " + 1/(elapsedTime / BATCH_SIZE))
                }
            }
        }

        // Handle remainder
        if (documents.size() > 0) {
            addDocuments(documents)
        }
        print "Done!\n"

        queue.shutdown()

        return nrImported
    }

    /*
    void printSpinner(int count) {
        print "Running dumpimport $count documents imported sofar. ${progressSpinner[progressSpinnerState]}                \r"
        progressSpinnerState = (progressSpinnerState + 1 >= progressSpinner.size() ? 0 : progressSpinnerState + 1)
    }
    */

    void addDocuments(final List documents) {
        queue.execute({
            try {
                this.whelk.bulkAdd(documents)
            } catch (WhelkAddException wae) {
                for (fi in wae.failedIdentifiers) {
                    failedLog << "$fi\n"
                }
            } catch (Exception e) {
                e.printStackTrace(new FileWriter(exceptionLog, true))
            }
        } as Runnable)
    }

    Document buildDocument(String mdrecord) {
        MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)

        String id = URLEncoder.encode(record.getControlfields("001").get(0).getData())
        String jsonRec = MarcJSONConverter.toJSONString(record)

        Document doc = null

        try {
            doc = new Document().withData(jsonRec.getBytes("UTF-8")).withEntry(["identifier":new String("/"+this.origin+"/"+id),"contentType":"application/x-marc-json"])
        } catch (Exception e) {
            log.error("Failed! (${e.message}) for :\n$mdrecord\nAs JSON:\n$jsonRec")
            if (picky) {
                throw e
            }
        }
        return doc
    }

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }
}
