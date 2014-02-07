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
import se.kb.libris.whelks.plugin.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import se.kb.libris.conch.converter.MarcJSONConverter

import static se.kb.libris.conch.Tools.*

@Log
class DumpImporter extends BasicPlugin implements Importer {

    Whelk whelk
    int nrImported = 0
    int nrDeleted = 0
    boolean picky, silent
    final int BATCH_SIZE = 1000
    String dataset
    int maxDocs

    String startTransformingAtElement


    File failedLog
    File exceptionLog

    ExecutorService queue

    DumpImporter(Map settings) {
        this.startTransformingAtElement = settings.get('startTransformingAtElement', null)
    }

    /*
    DumpImporter(Whelk toWhelk, String dataset, boolean picky = true) {
        this.whelk = toWhelk
        this.picky = picky
        this.dataset = dataset
    }
    */

    @Override
    int doImport(String dataset, int nrOfDocs, boolean silent, boolean picky, URL resource = null) {
        this.dataset = dataset
        this.picky = picky
        this.silent = silent
        this.maxDocs = nrOfDocs
        assert resource
        return doImportFromURL(resource)
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
        def documents = []
        Transformer optimusPrime = TransformerFactory.newInstance().newTransformer()
        long loadStartTime = System.nanoTime()

/* Snip */
      int event = xsr.getEventType();

      while (true) {
          Document doc = null
          if (event == XMLStreamConstants.START_ELEMENT && (!startTransformingAtElement || xsr.getLocalName() == startTransformingAtElement)) {
              try {
                  Writer outWriter = new StringWriter()
                  optimusPrime.transform(new StAXSource(xsr), new StreamResult(outWriter))
                  String xmlString = normalizeString(outWriter.toString())
                  doc = buildDocument(xmlString)
                  doc = whelk.sanityCheck(doc)
              } catch (javax.xml.stream.XMLStreamException xse) {
                  log.error("Skipping document, error in stream: ${xse.message}")
              }
              if (doc) {
                  documents << doc
                  if (log.isInfoEnabled() && !silent) {
                      printSpinner("Running dumpimport. $nrImported documents imported sofar.", nrImported)
                  }
                  if (++nrImported % BATCH_SIZE == 0) {
                      addDocuments(documents)
                      documents = []
                      float elapsedTime = ((System.nanoTime()-loadStartTime)/1000000000)
                      log.debug("imported: $nrImported time: $elapsedTime velocity: " + 1/(elapsedTime / BATCH_SIZE))
                  }
                  if (nrImported >= maxDocs) {
                      log.debug("Max number of docs ($maxDocs) reached. Breaking ...")
                      break
                  }
              }
          }

          if (!xsr.hasNext()) {
              break
          }
          event = xsr.next()
      }

      // Handle remainder
        if (documents.size() > 0) {
            addDocuments(documents)
        }

/*

        while(true) {
            log.debug("eventtype: $eventType, " + (eventType == XMLStreamConstants.START_ELEMENT ? xsr.getLocalName() : "unknown entity"))
            if (eventType == XMLStreamConstants.START_ELEMENT && xsr.getLocalName() == startTransformingAtElement) {
                log.debug("Found me a nice element to transform ...")
                Writer outWriter = new StringWriter()
                Document doc = null
                try {
                    optimusPrime.transform(new StAXSource(xsr), new StreamResult(outWriter))
                    String xmlString = normalizeString(outWriter.toString())
                    log.info("xmlString: $xmlString")
                    doc = buildDocument(xmlString)
                    doc = whelk.sanityCheck(doc)
                } catch (javax.xml.stream.XMLStreamException xse) {
                    log.error("Skipping document, error in stream: ${xse.message}")
                }

            } else {
                log.debug("Advancing ...")
                eventType = xsr.nextTag()
                if (eventType == XMLStreamConstants.START_ELEMENT) {
                    log.debug("nextTag yielded ${xsr.getLocalName()}")
                }
                while (eventType != XMLStreamConstants.START_ELEMENT) {
                    log.debug("Trodding through $eventType")
                    eventType = xsr.nextTag()
                }
            }
            if (!xsr.hasNext()) {
                log.debug("End of file. Stopping ..")
            }
        }
        log.debug("Done looping.")

        // Handle remainder
        if (documents.size() > 0) {
            addDocuments(documents)
        }
        */
        print "Done!\n"

        queue.shutdown()

        return nrImported
    }

    void addDocuments(final List documents) {
        queue.execute({
            try {
                this.whelk.bulkAdd(documents)
            } catch (WhelkAddException wae) {
                if (!failedLog) {
                    failedLog = new File("failed_ids.log")
                }
                for (fi in wae.failedIdentifiers) {
                    failedLog << "$fi\n"
                }
            } catch (Exception e) {
                if (!exceptionLog) {
                    exceptionLog = new File("exceptions.log")
                }
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
            doc = new Document().withData(jsonRec.getBytes("UTF-8")).withEntry(["identifier":new String("/"+this.dataset+"/"+id),"contentType":"application/x-marc-json"])
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
