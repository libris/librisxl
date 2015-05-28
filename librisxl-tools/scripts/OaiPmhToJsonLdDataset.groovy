import groovy.xml.XmlUtil
import org.codehaus.jackson.map.ObjectMapper
import whelk.importer.BasicOaiPmhImporter
import whelk.converter.MarcJSONConverter
import whelk.plugin.JsonLdToTurtle
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.plugin.libris.MarcFrameConverter

class OaiPmhToJsonLdDataset extends BasicOaiPmhImporter {

    int counter = 0
    String dest
    MarcFrameConverter converter
    ObjectMapper mapper
    OutputStream out
    String baseIri
    def jsonLdContext
    JsonLdToTurtle turtleSerializer

    OaiPmhToJsonLdDataset(dest, baseIri=null, contextPath=null) {
        this.dest = dest
        converter = new MarcFrameConverter()
        mapper = converter.mapper
        this.baseIri = baseIri
        if (contextPath) {
            def contextSrc = new File(contextPath).withInputStream {
                mapper.readValue(it, Map)
            }
            jsonLdContext = JsonLdToTurtle.parseContext(contextSrc)
        }
    }

    void run(startUrl, name, passwd) {
        new File(dest).withOutputStream {
            this.out = it
            if (jsonLdContext) {
                turtleSerializer = new JsonLdToTurtle(jsonLdContext, out, baseIri)
                turtleSerializer.prelude()
            }
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
                    String xmlRepr = XmlUtil.serialize(node)
                    String id = it.header.identifier
                    println "Saving #${++counter}: <${id}>"
                    marcXmlToRdfRepr(id, xmlRepr)
                } catch (Exception e) {
                    println "Error: $e"
                    println "Record: $it"
                }
            }
        }
    }

    void marcXmlToRdfRepr(String id, String xmlRepr) {
        def jsonld = marcXmlAsJsonLd(xmlRepr)
        if (turtleSerializer) {
            marcXmlToTurtle(id, jsonld)
        } else {
            def repr = mapper.writeValueAsString(jsonld)
            out.println(repr)
        }
    }

    void marcXmlToTurtle(String id, Map jsonld) {
        turtleSerializer.objectToTrig(id, jsonld)
    }

    Map marcXmlAsJsonLd(String xmlRepr) {
        def record = MarcXmlRecordReader.fromXml(xmlRepr)
        def source = MarcJSONConverter.toJSONMap(record)
        return converter.runConvert(source)
    }

    static void main(args) {
        def startUrl = args[0]
        def name = args[1]
        def passwd = args[2]
        def dest = args[3]
        def base = args.length >= 5? args[4] : null
        def ctxPath = args.length >= 6? args[5] : null
        new OaiPmhToJsonLdDataset(dest, base, ctxPath).run(startUrl, name, passwd)
    }

}
