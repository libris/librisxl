package se.kb.libris.whelks.importers

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

@Log
class LibrisOaiPmhImporter extends OaiPmhImporter implements Importer {
    Whelk whelk
    String id
    List<Plugin> plugins = new ArrayList<Plugin>()
    Map global

    String serviceUrl = "http://data.libris.kb.se/{dataset}/oaipmh"

    def specUriMapping = [:]
    def datasetMapping = [:]
    boolean preserveTimestamps = false

    LibrisOaiPmhImporter(Map settings) {
        preserveTimestamps = settings.get("preserveTimestamps", preserveTimestamps)
        specUriMapping = settings.get("specUriMapping", specUriMapping)
        datasetMapping = settings.get("datasetMapping", datasetMapping)
    }

    void parseResult(final OAIPMH) {
        def docs = []
        OAIPMH.ListRecords.record.each {
            String mdrecord = createString(it.metadata.record)
            String dataset = datasetMapping[it.metadata.record.@type.toString()] ?: it.metadata.record.@type.toString()
            String identifier = "/" + new URI(it.header.identifier.text()).getPath().split("/")[2 .. -1].join("/")
            assert identifier
            if (mdrecord) {
                assert dataset
                def entry = ["identifier":identifier,"dataset":"hold","contentType":"application/marcxml+xml"]
                def meta = [:]

                if (preserveTimestamps && it.header.datestamp) {
                    def date = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", it.header.datestamp.toString())
                    entry.put("modified", date.getTime())
                }

                for (spec in it.header.setSpec) {
                    String link = new String("/"+(specUriMapping[spec.toString().split(":")[0]] ?: spec.toString().split(":")[0])+"/" + spec.toString().split(":")[1])
                    log.trace("Built link $link")
                    meta.get("link", []).add(link)
                }

                docs << new Document(["entry":entry, "meta":meta]).withData(mdrecord)
            } else if (it.header.@deleted == 'true') {
                println "Delete record $identifier"
                whelk.remove(new URI(identifier))
            }
        }
        if (!docs.isEmpty()) {
            whelk.bulkAdd(docs, docs.first().contentType)
        }
    }


    int doImport(String ds, String token, int maxNrOfDocsToImport, boolean silent, boolean picky, Date since = null) {
        String startUrl = serviceUrl.replace("{dataset}", ds)
        Properties properties = new Properties()
        properties.load(this.getClass().getClassLoader().getResourceAsStream("oaipmh.properties"))
        String username = properties.getProperty("username")
        String password = properties.getProperty("password")
        parseOaipmh(startUrl, username, password, since)
        return recordCount
    }

    void init(String str) {}
    void cancel() { ok = false }

    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }
    public Map getGlobal() { global }
}
