package whelk.importer.swepub

import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult
import groovy.util.logging.Slf4j as Log

import whelk.importer.*
import whelk.*

@Log
class ModsImporter extends DumpImporter {

    ModsImporter(Map settings) {
        super(settings)
    }

    @Override
    Document buildDocument(String xmlData) {
        def record = new XmlSlurper(false,false).parseText(xmlData)
        def identifier = "/"+this.dataset+"/"+record.header.identifier.text()
        def datestamp = Date.parse("yyyy-MM-dd'T'hh:mm:ss'Z'", record.header.datestamp.text())
        def sets = record.header.setSpec.list().collect { it.text() }
        log.info("Record is $record")
        log.info("Identifier: $identifier")
        log.info("Datestamp: $datestamp")
        log.info("SetSpec: $sets")
        return new Document()
            .withIdentifier(identifier)
            .withData(createString(record.metadata.mods).getBytes("UTF-8"))
            .withTimestamp(datestamp.getTime())
            .withEntry([
                "contentType":"application/mods+xml"
                ])
            .withMeta([
                "sets":sets
            ])
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }
}
