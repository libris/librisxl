import groovy.xml.XmlUtil
import whelk.importer.BasicOaiPmhImporter
import whelk.converter.MarcJSONConverter
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.plugin.libris.MarcFrameConverter

class OaiPmhToJsonLdDataset extends BasicOaiPmhImporter {

    int counter = 0
    String dest
    MarcFrameConverter converter
    PrintWriter pw

    OaiPmhToJsonLdDataset(dest) {
        this.dest = dest
        converter = new MarcFrameConverter()
    }

    void run(startUrl, name, passwd) {
        new File(dest).withPrintWriter {
            this.pw = it
            parseOaipmh(startUrl, name, passwd, null)
        }
    }

    @Override
    def makeNextUrl(startUrl, resumptionToken, since) {
        def next = super.makeNextUrl(startUrl, resumptionToken, since)
        println "Following: <$next>"
        return next
    }

    void parseResult(final slurpedXml) {
        slurpedXml.ListRecords.record.each {
            if (it.header?.@status == 'deleted' || it.header?.@deleted == 'true') {
                println "Skipping deleted"
            } else {
                def node = it.metadata.record
                try {
                    def xmlRepr = XmlUtil.serialize(node)
                    def data = marcXmlToJsonLd(xmlRepr)
                    println "Saving #${++counter}: <${it.header.identifier}>"
                    saveData(data)
                } catch (Exception e) {
                    println "Error: $e"
                    println "Record: $it"
                }
            }
        }
    }

    Map marcXmlToJsonLd(String xmlRepr) {
        def record = MarcXmlRecordReader.fromXml(xmlRepr)
        def source = MarcJSONConverter.toJSONMap(record)
        return converter.runConvert(source)
    }

    void saveData(Map data) {
        pw.println(converter.mapper.writeValueAsString(data))
    }

    static void main(args) {
        def startUrl = args[0]
        def name = args[1]
        def passwd = args[2]
        def dest = args[3]
        new OaiPmhToJsonLdDataset(dest).run(startUrl, name, passwd)
    }

}
