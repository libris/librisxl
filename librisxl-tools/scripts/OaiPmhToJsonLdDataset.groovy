import groovy.xml.XmlUtil
import whelk.importer.BasicOaiPmhImporter
import whelk.converter.MarcJSONConverter
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.plugin.libris.MarcFrameConverter

class OaiPmhToJsonLdDataset extends BasicOaiPmhImporter {

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

    void parseResult(final slurpedXml) {
        slurpedXml.ListRecords.record.each {
            def node = it.metadata.record
            def xmlRepr = XmlUtil.serialize(node)
            def data = marcXmlToJsonLd(xmlRepr)
            println "Saving: ${it.header.identifier}"
            saveData(data)
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
