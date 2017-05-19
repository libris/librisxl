package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult
import org.codehaus.jackson.map.ObjectMapper
import whelk.converter.marc.MarcFrameConverter
import whelk.rest.ServiceServlet

import java.util.concurrent.*
import javax.servlet.http.*

import whelk.*
import whelk.component.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import whelk.converter.*

import java.util.regex.Pattern

@Log
class RemoteSearchAPI extends HttpServlet {
    final static mapper = new ObjectMapper()

    String description = "Query API for remote search"

    Map remoteURLs

    Pattern pathPattern = Pattern.compile("/_remotesearch")

    MarcFrameConverter marcFrameConverter

    URL metaProxyInfoUrl = new URL("http://mproxy.libris.kb.se/db_Metaproxy.xml")
    String metaProxyBaseUrl = "http://mproxy.libris.kb.se:8000"

    final String DEFAULT_DATABASE = "LC"

    def urlParams = ["version": "1.1", "operation": "searchRetrieve", "maximumRecords": "10","startRecord": "1"]

    RemoteSearchAPI() {
        log.info("Starting Remote Search API")
        loadMetaProxyInfo(metaProxyInfoUrl)
        marcFrameConverter = new MarcFrameConverter()
    }

    List loadMetaProxyInfo(URL url) {
        List databases = []
        try {
            def xml = new XmlSlurper(false, false).parse(url.newInputStream())

            databases = xml.libraryCode.collect {
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

            remoteURLs = databases.inject([:]) { map, db ->
                map << [(db.database): metaProxyBaseUrl + "/" + db.database]
            }
        } catch (SocketException se) {
            log.error("Unable to load database list.")
        }
        return databases
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        log.info("Performing remote search ...")
        def query = request.getParameter("q")
        int start = (request.getParameter("start") ?: "0") as int
        int n = (request.getParameter("n") ?: "10") as int
        def databaseList = (request.getParameter("databases") ?: DEFAULT_DATABASE).split(",") as List
        def queryStr, url
        MarcRecord record
        String output = ""

        urlParams['maximumRecords'] = n
        urlParams['startRecord'] = (start < 1 ? 1 : start)

        log.trace("Query is $query")
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
                        futures << queue.submit(new MetaproxyQuery(url, database))
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
                def results = ['totalResults':[:],'items':[]]
                int biggestList = 0
                for (result in resultLists) { if (result.hits.size() > biggestList) { biggestList = result.hits.size() } }
                def errors = [:]
                for (int i = 0; i <= biggestList; i++) {
                    for (result in resultLists) {
                        results.totalResults[result.database] = result.numberOfHits
                        try {
                            if (result.error) {
                                errors.get(result.database, [:]).put(""+i, result.error)
                            } else if (i < result.hits.size()) {
                                results.items << ['database':result.database,'data':result.hits[i].data]
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
            HttpTools.sendResponse(response, output, "application/json")
        }
    }

    class MetaproxyQuery implements Callable<MetaproxySearchResult> {

        URL url
        String database

        MetaproxyQuery(URL queryUrl, String db) {
            this.url = queryUrl
            this.database = db
        }

        @Override
        MetaproxySearchResult call() {
            def docStrings, results
            try {
                log.debug("requesting data from url: $url")
                def xmlRecords = new XmlSlurper().parseText(url.text).declareNamespace(zs:"http://www.loc.gov/zing/srw/", tag0:"http://www.loc.gov/MARC21/slim", diag:"http://www.loc.gov/zing/srw/diagnostic/")
                def errorMessage = null
                int numHits = 0
                try {
                    numHits = xmlRecords.'zs:numberOfRecords'.toInteger()
                } catch (NumberFormatException nfe) {
                    def emessageElement = xmlRecords.'zs:diagnostics'.'diag:diagnostic'.'diag:message'
                    errorMessage = emessageElement.text()
                }
                if (!errorMessage) {
                    docStrings = getXMLRecordStrings(xmlRecords)
                    results = new MetaproxySearchResult(database, numHits)
                    for (docString in docStrings) {
                        def record = MarcXmlRecordReader.fromXml(docString)
                        // Not always available (and also unreliable)
                        //id = record.getControlfields("001").get(0).getData()

                        log.trace("Marcxmlrecordreader for done")

                        def jsonRec = MarcJSONConverter.toJSONString(record)
                        log.trace("Marcjsonconverter for done")
                        def xMarcJsonDoc = new Document().withData(mapper.readValue(jsonRec.getBytes("UTF-8"), Map)).withContentType("application/x-marc-json")
                        //Convert xMarcJsonDoc to ld+json
                        def jsonDoc = marcFrameConverter.convert(xMarcJsonDoc)
                        /* Necessary?
                        if (!jsonDoc.id) {
                            jsonDoc.id = new URIMinter().mint(jsonDoc)
                        }
                        */
                        log.trace("Marcframeconverter done")

                        results.addHit(jsonDoc)
                    }
                } else {
                    log.warn("Received errorMessage from metaproxy: $errorMessage")
                    results = new MetaproxySearchResult(database, 0)
                    results.error = errorMessage
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

    class MetaproxySearchResult {

        List hits = new ArrayList()
        String database, error
        int numberOfHits

        MetaproxySearchResult(String db, int nrHits) {
            this.numberOfHits = nrHits
            this.database = db
        }

        void addHit(Document doc) {
            hits.add(doc)
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
