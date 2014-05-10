package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

import java.util.concurrent.*
import javax.servlet.http.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*

@Log
class RemoteSearchAPI extends BasicAPI {
    final static mapper = new ElasticJsonMapper()

    String description = "Query API for remote search"
    String id = "RemoteSearchAPI"

    Map remoteURLs

    MarcFrameConverter marcFrameConverter

    URL metaProxyInfoUrl
    String metaProxyBaseUrl

    final String DEFAULT_DATABASE = "LC"

    def urlParams = ["version": "1.1", "operation": "searchRetrieve", "maximumRecords": "10","startRecord": "1"]

    RemoteSearchAPI(Map settings) {
        this.metaProxyBaseUrl = settings.metaproxyBaseUrl
        // Cut trailing slashes from url
        while (metaProxyBaseUrl.endsWith("/")) {
            metaProxyBaseUrl = metaProxyBaseUrl[0..-1]
        }
        assert metaProxyBaseUrl
        metaProxyInfoUrl = new URL(settings.metaproxyInfoUrl)

        // Prepare remoteURLs by loading settings once.
        loadMetaProxyInfo(metaProxyInfoUrl)
    }


    List loadMetaProxyInfo(URL url) {
        def xml = new XmlSlurper(false,false).parse(url.newInputStream())

        def databases = xml.libraryCode.collect {
            def map = ["database": createString(it.@id)]
            it.children().each { node ->
                def n = node.name().toString()
                def o = node.text().toString()
                def v = map[n]
                if (v) {
                    if (v instanceof String) {
                        v = map[n] = [v]
                    }
                    v << o
                } else {
                    map[n] = o
                }
            }
            return map
        }

        remoteURLs = databases.inject( [:] ) { map, db ->
            map << [(db.database) : metaProxyBaseUrl + "/" + db.database]
        }

        return databases
    }


    void init(String wn) {
        log.debug("plugins: ${plugins}")
        marcFrameConverter = plugins.find { it instanceof FormatConverter && it.resultContentType == "application/ld+json" && it.requiredContentType == "application/x-marc-json" }
        assert marcFrameConverter
    }

    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        def query = request.getParameter("q")
        int start = (request.getParameter("start") ?: "0") as int
        int n = (request.getParameter("n") ?: "10") as int
        def databaseList = (request.getParameter("database") ?: DEFAULT_DATABASE).split(",") as List
        def queryStr, url
        MarcRecord record
        OaiPmhXmlConverter oaiPmhXmlConverter
        String output = ""

        urlParams['maximumRecords'] = n
        urlParams['startRecord'] = (start < 1 ? 1 : start)

        if (query) {
            // Weed out the unavailable databases
            databaseList = databaseList.intersect(remoteURLs.keySet() as List)
            log.debug("Remaining databases: $databaseList")
            urlParams.each { k, v ->
                if (!queryStr) {
                    queryStr = "?"
                } else {
                    queryStr += "&"
                }
                queryStr += k + "=" + v
            }
            if (!databaseList) {
                output = "{\"error\":\"Requested database is unknown or unavailable.\"}"
            }
            else {
                ExecutorService queue = Executors.newCachedThreadPool()
                def resultLists = []
                try {
                    def futures = []
                    for (database in databaseList) {
                        url = new URL(remoteURLs[database] + queryStr + "&query=" + URLEncoder.encode(query, "utf-8"))
                        log.debug("submitting to futures")
                        futures << queue.submit(new MetaproxyQuery(whelk, url, database))
                    }
                    log.info("Started all threads. Now harvesting the future")
                    for (f in futures) {
                        log.debug("Waiting for future ...")
                        def sr = f.get()
                        log.debug("Adding ${sr.numberOfHits} to ${resultLists}")
                        resultLists << sr
                    }
                } finally {
                    queue.shutdown()
                }
                // Merge results
                def results = ['hits':[:],'list':[]]
                int biggestList = 0
                for (result in resultLists) { if (result.hits.size() > biggestList) { biggestList = result.hits.size() } }
                def errors = [:]
                for (int i = 0; i <= biggestList; i++) {
                    for (result in resultLists) {
                        results.hits[result.database] = result.numberOfHits
                        try {
                            if (result.error) {
                                errors.get(result.database, [:]).put(""+i, result.error)
                            } else if (i < result.hits.size()) {
                                results.list << ['database':result.database,'data':result.hits[i].dataAsMap]
                            }
                        } catch (ArrayIndexOutOfBoundsException aioobe) {
                            log.debug("Overstepped array bounds.")
                        } catch (NullPointerException npe) {
                            log.trace("npe.")
                        }
                    }
                }
                if (errors) {
                    results['errors'] = errors
                }
                output = mapper.writeValueAsString(results)
            }
        } else if (request.getParameter("databases")) {
            def databases = loadMetaProxyInfo(metaProxyInfoUrl)
            output = mapper.writeValueAsString(databases)
        } else if (!query) {
            output = "{\"error\":\"Use parameter 'q'\"}"
        }
        if (!output) {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT)
        } else {
            sendResponse(output, "application/json")
        }
    }

    class MetaproxyQuery implements Callable<MetaproxySearchResult> {

        URL url
        String database
        Whelk whelk

        MetaproxyQuery(Whelk w, URL queryUrl, String db) {
            this.url = queryUrl
            this.database = db
            this.whelk = w
            assert whelk
        }

        @Override
        MetaproxySearchResult call() {
            def docStrings, results
            try {
                log.debug("requesting data from url: $url")
                def xmlRecords = new XmlSlurper().parseText(url.text).declareNamespace(zs:"http://www.loc.gov/zing/srw/", tag0:"http://www.loc.gov/MARC21/slim")
                int numHits = xmlRecords.'zs:numberOfRecords'.toInteger()
                docStrings = getXMLRecordStrings(xmlRecords)
                results = new MetaproxySearchResult(database, numHits)
                for (docString in docStrings) {
                    def record = MarcXmlRecordReader.fromXml(docString)
                    // Not always available (and also unreliable)
                    //id = record.getControlfields("001").get(0).getData()

                    log.trace("Marcxmlrecordreader for done")

                    def jsonRec = MarcJSONConverter.toJSONString(record)
                    log.trace("Marcjsonconverter for done")
                    def xMarcJsonDoc = new Document()
                    .withData(jsonRec.getBytes("UTF-8"))
                    .withContentType("application/x-marc-json")
                    //Convert xMarcJsonDoc to ld+json
                    def jsonDoc = marcFrameConverter.doConvert(xMarcJsonDoc)
                    if (!jsonDoc.identifier) {
                        jsonDoc.identifier = this.whelk.mintIdentifier(jsonDoc)
                    }
                    log.trace("Marcframeconverter done")

                    results.addHit(jsonDoc)
                }
            } catch (org.xml.sax.SAXParseException spe) {
                log.error("Failed to parse XML: ${url.text}")
                results = new MetaproxySearchResult(database, 0)
                results.error = "Failed to parse XML (${spe.message}))"
            } catch (java.net.ConnectException ce) {
                log.error("Connection failed", ce)
                results = new MetaproxySearchResult(database, 0)
                results.error = "Connection with metaproxy failed."
            } catch (Exception e) {
                log.error("Could not convert document from $docStrings", e)
                results = new MetaproxySearchResult(database, 0)
                results.error = "Failed to convert results to JSON."
            }
            return results
        }
    }

    class MetaproxySearchResult extends SearchResult {

        String database, error

        MetaproxySearchResult(String db, int nrHits) {
            super(nrHits)
            this.database = db
        }
    }

    List<String> getXMLRecordStrings(xmlRecords) {
        def xmlRecs = new ArrayList<String>()
        def allRecords = xmlRecords?.'zs:records'?.'zs:record'
        for (record in allRecords) {
            def recordData = record.'zs:recordData'
            def marcXmlRecord = createString(recordData.'tag0:record')
            if (marcXmlRecord) {
                xmlRecs << removeNamespacePrefixes(marcXmlRecord)
            }
        }
        return xmlRecs
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind {
            out << root
        }
    }

    String removeNamespacePrefixes(String xmlStr) {
        return xmlStr.replaceAll("tag0:", "").replaceAll("zs:", "")
    }
}
