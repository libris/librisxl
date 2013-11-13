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

    OaiPmhXmlConverter(Map settings) {
        this.SPEC_URI_MAP = settings.get("specUriMapping", [:])
    }

    Document doConvert(final Document document) {
        def xml = new XmlSlurper(false,false).parseText(document.dataAsString)
        String rstring = createString(xml.metadata.record)
        MarcRecord record = MarcXmlRecordReader.fromXml(rstring)

        String jsonRec = MarcJSONConverter.toJSONString(record)

        def doc = new Document()
            .withData(jsonRec.getBytes("UTF-8"))
            .withIdentifier(document.identifier)
            .withEntry(document.entry)
            .withMeta(document.meta)

        if (xml.header.setSpec) {
            for (spec in xml.header.setSpec) {
                for (key in SPEC_URI_MAP.keySet()) {
                    if (spec.toString().startsWith(key+":")) {
                        doc.withLink(new String("/"+SPEC_URI_MAP[key]+"/" + spec.toString().substring(key.length()+1)), key)
                    }
                }
            }
        }

        return doc.withContentType(resultContentType)
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }
}
