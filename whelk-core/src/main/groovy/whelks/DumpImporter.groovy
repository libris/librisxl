package se.kb.libris.whelks.importers

import groovy.xml.StreamingMarkupBuilder
import groovy.util.logging.Slf4j as Log
import groovy.time.*

import java.text.*

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

@Log
class DumpImporter {

    Whelk whelk
    int nrImported = 0
    int nrDeleted = 0
    boolean picky
    final int BATCH_SIZE = 1000

    DumpImporter(Whelk toWhelk, boolean picky = true) {
        this.whelk = toWhelk
        this.picky = picky
    }

    int doImportFromFile(File file) {
        //def file = "/data/librisxl/${whelk.prefix}.xml"
        log.info("Loading dump from $file")
        XMLInputFactory xif = XMLInputFactory.newInstance()
        XMLStreamReader xsr = xif.createXMLStreamReader(new FileReader(file))
        return performImport(xsr)
    }

    int doImportFromURL(URL url) {
        def properties = new Properties()
        properties.load(this.getClass().getClassLoader().getResourceAsStream("whelks-core.properties"))
        def urlString = properties.getProperty("dumpurl_${whelk.prefix}")
        log.info("Loading dump from $urlString")
        XMLInputFactory xif = XMLInputFactory.newInstance()
        //XMLStreamReader xsr = xif.createXMLStreamReader(new URL(urlString).newInputStream())
        XMLStreamReader xsr = xif.createXMLStreamReader(url.newInputStream())
        return performImport(xsr)
    }

    int performImport(XMLStreamReader xsr) {
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
            } catch (javax.xml.stream.XMLStreamException xse) {
                log.error("Skipping document, error in stream: ${xse.message}")
            }

            if (doc) {
                documents << doc
                if (++nrImported % BATCH_SIZE == 0) {
                    this.whelk.bulkStore(documents)
                    documents = []
                    //def td = TimeCategory.minus(new Date(), loadStartTime)
                    //log.debug("$nrImported documents stored. Lap time is $td")
                    float elapsedTime = ((System.nanoTime()-loadStartTime)/1000000000)
                    log.debug("imported: $nrImported time: $elapsedTime velocity: " + 1/(elapsedTime / BATCH_SIZE))
                }
            }
        }

        // Handle remainder
        if (documents.size() > 0) {
            whelk.bulkStore(documents)
        }

        return nrImported
    }

    Document buildDocument(String mdrecord) {
        MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)

        String id = URLEncoder.encode(record.getControlfields("001").get(0).getData())
        String jsonRec = MarcJSONConverter.toJSONString(record)

        Document doc = null

        try {
            doc = whelk.createDocument(jsonRec.getBytes("UTF-8"), ["identifier":new URI("/"+whelk.prefix+"/"+id),"contentType":"application/json","format":"marc21"])
        } catch (Exception e) {
            log.error("Failed! (${e.message}) for :\n$mdrecord")
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
