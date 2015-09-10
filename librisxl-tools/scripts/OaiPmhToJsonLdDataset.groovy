import groovy.xml.XmlUtil
import org.codehaus.jackson.map.ObjectMapper
import whelk.importer.BasicOaiPmhImporter
import whelk.converter.MarcJSONConverter
import whelk.plugin.JsonLdToTurtle
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.plugin.libris.MarcFrameConverter

class OaiPmhToJsonLdDataset extends BasicOaiPmhImporter {

    static int limit = -1

    int counter = 0
    String dest
    MarcFrameConverter converter
    ObjectMapper mapper
    Writer writer
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
            if (jsonLdContext) {
                turtleSerializer = new JsonLdToTurtle(jsonLdContext, it, baseIri)
                turtleSerializer.prelude()
            } else {
                writer = it.newWriter('UTF-8')
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
            if (limit > -1 && counter == limit)
                throw new BreakException()
        }
    }

    void marcXmlToRdfRepr(String id, String xmlRepr) {
        def jsonld = marcXmlAsJsonLd(xmlRepr)
        if (turtleSerializer) {
            marcXmlToTurtle(id, jsonld)
        } else {
            def repr = mapper.writeValueAsString(jsonld)
            writer.write(repr)
            writer.write('\n')
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

    static void main(argv) {
        def cli = new CliBuilder(usage:'SCRIPT [OPTS] START_URL DEST_FILE')
        cli.u(args:1, 'set user:password')
        cli.l(args:1, 'set limit')
        cli.b(args:1, 'set base')
        cli.c(args:1, 'set context for Trig output')
        cli.p('use profiler')
        def opts = cli.parse(argv)
        def args = opts.arguments()

        if (args.size() == 0) {
            println cli.usage()
            System.exit(0)
        }

        def startUrl = args[0]
        def dest = args[1]
        def (name, passwd) = (opts.u ?: " : ").split(':').collect { it.trim() }
        def base = opts.b ?: null
        def ctxPath = opts.c ?: null
        if (opts.l) {
            OaiPmhToJsonLdDataset.limit = Integer.parseInt(opts.l)
            println "Limit set to $opts.l"
        }

        def profiler = opts.p? new groovyx.gprof.Profiler() : null

        if (profiler) {
            println "Using profiler"
            profiler.start()
        }

        try {
            new OaiPmhToJsonLdDataset(dest, base, ctxPath).run(startUrl, name, passwd)
        } catch (BreakException e) {
            println "Break"
        }

        if (profiler) {
            profiler.stop()
            println ">" * 42
            profiler.report.flat.prettyPrint()
            println "<" * 42
        }
    }

    static class BreakException extends Exception {}

}
