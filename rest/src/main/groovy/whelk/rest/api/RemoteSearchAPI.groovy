package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import groovy.util.slurpersupport.GPathResult
import groovy.xml.StreamingMarkupBuilder
import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.util.marc.Field
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.Document
import whelk.IdGenerator
import whelk.Whelk
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.util.WhelkFactory

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Log
class RemoteSearchAPI extends HttpServlet {

    final static mapper = new ObjectMapper()

    MarcFrameConverter marcFrameConverter

    static final URL metaProxyInfoUrl = new URL("http://mproxy.libris.kb.se/db_Metaproxy.xml")
    static final String metaProxyBaseUrl = "http://mproxy.libris.kb.se:8000"

    final String DEFAULT_DATABASE = "LC"

    private Whelk whelk

    private Set<String> m_undesirableFields

    RemoteSearchAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    @Override
    void init() {
        log.info("Starting Remote Search API")
        if (!whelk) {
            whelk = WhelkFactory.getSingletonWhelk()
        }
        marcFrameConverter = whelk.getMarcFrameConverter()

        m_undesirableFields = new HashSet<>()

        // Ignored fields
        m_undesirableFields.add("013")
        m_undesirableFields.add("018")
        m_undesirableFields.add("031")
        m_undesirableFields.add("049")
        m_undesirableFields.add("061")
        m_undesirableFields.add("066")
        m_undesirableFields.add("071")
        m_undesirableFields.add("085")
        for (int i = 89; i <= 99; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        m_undesirableFields.add("270")
        m_undesirableFields.add("307")
        m_undesirableFields.add("355")
        m_undesirableFields.add("357")
        for (int i = 363; i <= 388; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        m_undesirableFields.add("514")
        m_undesirableFields.add("526")
        m_undesirableFields.add("542")
        m_undesirableFields.add("552")
        m_undesirableFields.add("565")
        m_undesirableFields.add("567")
        m_undesirableFields.add("584")
        m_undesirableFields.add("654")
        for (int i = 656; i <= 658; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        m_undesirableFields.add("662")
        for (int i = 863; i <= 878; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }

        // Unhandled fields
        m_undesirableFields.add("009")
        m_undesirableFields.add("011")
        m_undesirableFields.add("012")
        m_undesirableFields.add("014")
        m_undesirableFields.add("019")
        m_undesirableFields.add("021")
        m_undesirableFields.add("023")
        m_undesirableFields.add("029")
        m_undesirableFields.add("039")
        m_undesirableFields.add("053")
        m_undesirableFields.add("054")
        for (int i = 56; i <= 59; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 62; i <= 65; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 67; i <= 69; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        m_undesirableFields.add("073")
        for (int i = 75; i <= 79; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        m_undesirableFields.add("081")
        m_undesirableFields.add("087")
        for (int i = 189; i <= 199; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 289; i <= 299; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 389; i <= 399; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 491; i <= 499; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 589; i <= 598; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 689; i <= 699; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        m_undesirableFields.add("758")
        for (int i = 831; i <= 855; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 882; i <= 885; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }
        for (int i = 887; i <= 899; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }

        // Local fields
        for (int i = 900; i <= 999; ++i) {
            m_undesirableFields.add(String.format("%1\$03d", i))
        }

        log.info("Started ...")
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
        } catch (SocketException se) {
            log.error("Unable to load database list.")
        }
        return databases
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        // Check that we have kat-rights.
        boolean hasPermission = false
        Map userInfo = request.getAttribute("user")
        if (userInfo != null) {
            if (userInfo.permissions.any { item ->
                item.get(whelk.rest.security.AccessControl.KAT_KEY)
            } || Crud.isSystemUser(userInfo))
                hasPermission = true
        }

        if (hasPermission){
            response.sendError(HttpServletResponse.SC_FORBIDDEN)
            return
        }

        log.info("Performing remote search ...")
        def query = request.getParameter("q")
        int start = (request.getParameter("start") ?: "0") as int
        int n = (request.getParameter("n") ?: "10") as int
        def databaseList = (request.getParameter("databases") ?: DEFAULT_DATABASE).split(",") as List
        def queryStr, url
        String output = ""

        def urlParams = ["version": "1.1", "operation": "searchRetrieve"]
        urlParams['maximumRecords'] = n
        urlParams['startRecord'] = (start < 1 ? 1 : start)

        log.trace("Query is $query")
        if (query) {
            Map remoteURLs = loadMetaProxyInfo(metaProxyInfoUrl).inject([:]) { map, db ->
                map << [(db.database): metaProxyBaseUrl + "/" + db.database]
            }

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
                output = mergeResults(resultLists)
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

    String mergeResults(List<MetaproxySearchResult> resultsList) {
        def results = ['totalResults': [:], 'items': []]
        def errors = [:]

        getRange(resultsList).collect { index ->
            resultsList.each { result ->
                if (result.error) {
                    errors.get(result.database, [:]).put("" + index, result.error)
                } else if (result.hits[index]) {
                    results.items << ['database': result.database, 'data': result.hits[index].data]
                }
            }
        }

        resultsList.each { result ->
            results.totalResults[result.database] = result.numberOfHits
        }

        if (errors) {
            results.errors = errors
        }

        return mapper.writeValueAsString(results)
    }

    private List getRange(List resultsList) {
        def hitList = resultsList*.hits
        def largestNumberOfHits = hitList*.size().max()
        return  0..<largestNumberOfHits
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

                        String generatedId = sanitizeMarcAndGenerateNewID(record)

                        log.trace("Marcxmlrecordreader for done")
                        def jsonRec = MarcJSONConverter.toJSONString(record)
                        log.trace("Marcjsonconverter for done")
                        def jsonDoc = marcFrameConverter.convert(mapper.readValue(jsonRec.getBytes("UTF-8"), Map), generatedId)
                        log.trace("Marcframeconverter done")

                        results.addHit(new Document(jsonDoc))
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

    String sanitizeMarcAndGenerateNewID(MarcRecord marcRecord) {
        List<Field> mutableFieldList = marcRecord.getFields()

        // Replace any existing 001 fields, TODO: move 001 to 035$a instead?
        String generatedId = IdGenerator.generate()
        if (marcRecord.getControlfields("001").size() != 0) {
            mutableFieldList.remove(marcRecord.getControlfields("001").get(0))
        }
        marcRecord.addField(marcRecord.createControlfield("001", generatedId))

        // Remove unwanted marc fields.
        Iterator<Field> it = mutableFieldList.iterator()
        while (it.hasNext()) {
            Field field = it.next()
            String fieldNumber = field.getTag()
            if (m_undesirableFields.contains(fieldNumber))
                it.remove()
        }
        return generatedId
    }
}
