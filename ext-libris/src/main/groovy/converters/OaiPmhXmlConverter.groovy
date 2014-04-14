package se.kb.libris.whelks.plugin

import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult
import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import se.kb.libris.conch.converter.MarcJSONConverter

@Log
class OaiPmhXmlConverter extends BasicFormatConverter {
    String requiredContentType = "text/oaipmh+xml"
    String resultContentType = "application/x-marc-json"

    def SPEC_URI_MAP
    boolean preserveTimestamps

    OaiPmhXmlConverter(Map settings) {
        this.SPEC_URI_MAP = settings.get("specUriMapping", [:])
        this.preserveTimestamps = settings.get("preserveTimestamps", false)
    }

    Document doConvert(final Document document) {
        long elapsed = System.currentTimeMillis()
        def xml = new XmlSlurper(false,false).parseText(document.dataAsString)
        if ((System.currentTimeMillis() - elapsed) > 1000) {
            log.warn("XML parsing took more than 1 seconds (${System.currentTimeMillis() - elapsed})")
        }
        elapsed = System.currentTimeMillis()
        String rstring = createString(xml.metadata.record)
        if ((System.currentTimeMillis() - elapsed) > 1000) {
            log.warn("CreateString took more than 1 seconds (${System.currentTimeMillis() - elapsed})")
        }
        MarcRecord record = MarcXmlRecordReader.fromXml(rstring)


        log.debug("Creating new document ${document.identifier} from doc with entry: ${document.entry} and meta: ${document.meta}")

        elapsed = System.currentTimeMillis()
        String jsonRec = MarcJSONConverter.toJSONString(record)

        def doc = new Document()
            .withData(jsonRec.getBytes("UTF-8"))
            .withIdentifier(document.identifier)
            .withEntry(document.entry)
            .withMeta(document.meta)

        if (preserveTimestamps && xml.header.datestamp) {
            def date = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", xml.header.datestamp.toString())
            log.trace("Setting date: $date")
            doc.timestamp = date.getTime()
        }

        if (xml.header.setSpec) {
            for (spec in xml.header.setSpec) {
                for (key in SPEC_URI_MAP.keySet()) {
                    if (spec.toString().startsWith(key+":")) {
                        def link = new String("/"+SPEC_URI_MAP[key]+"/" + spec.toString().substring(key.length()+1))
                        log.trace("Adding link $link ($key) to ${doc.identifier}")
                        doc.withLink(link, SPEC_URI_MAP[key])
                    }
                }
            }
        }
        if ((System.currentTimeMillis() - elapsed) > 1000) {
            log.warn("Document creation took more than 1 seconds (${System.currentTimeMillis() - elapsed})")
        }
        log.debug("Document ${doc.identifier} created successfully with entry: ${doc.entry} and meta: ${doc.meta}")

        return doc.withContentType(resultContentType)
    }

    String createString(Node root) {
        def writer = new StringWriter()
        new XmlNodePrinter(new PrintWriter(writer)).print(root)
        return writer.toString()
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind {
            out << root
        }
    }
}
