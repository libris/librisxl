package whelk.rest.api;

import groovy.xml.StreamingMarkupBuilder;
import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.utils.isbn.IsbnParser;
import whelk.Document;
import whelk.IdGenerator;
import whelk.Whelk;
import whelk.converter.MarcJSONConverter;
import whelk.converter.marc.MarcFrameConverter;
import whelk.filter.LinkFinder;
import whelk.util.LegacyIntegrationTools;
import whelk.util.PropertyLoader;
import whelk.util.http.HttpTools;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static whelk.util.Jackson.mapper;

public class RemoteSearchAPI extends WhelkHttpServlet {
    private static final Logger log = LogManager.getLogger(RemoteSearchAPI.class);

    MarcFrameConverter marcFrameConverter;
    static final URL metaProxyInfoUrl;
    static final String metaProxyBaseUrl;
    static {
        try {
            Properties props = PropertyLoader.loadProperties("secret");
            metaProxyInfoUrl = new URL(props.getProperty("metaProxyInfoUrl", "http://mproxy.libris.kb.se/db_Metaproxy.xml"));
            metaProxyBaseUrl = props.getProperty("metaProxyBaseUrl", "http://mproxy.libris.kb.se:8000");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RemoteSearchAPI", e);
        }
    }

    final String DEFAULT_DATABASE = "LC";

    private LinkFinder linkFinder;

    private Set<String> m_undesirableFields;
    private Set<String> m_undesirableFieldsAuth;

    public RemoteSearchAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    @Override
    protected void init(Whelk whelk) {
        log.info("Starting Remote Search API");
        marcFrameConverter = whelk.getMarcFrameConverter();
        linkFinder = new LinkFinder(whelk.getStorage());

        m_undesirableFields = new HashSet<>();

        // Ignored fields
        m_undesirableFields.add("013");
        m_undesirableFields.add("018");
        m_undesirableFields.add("031");
        m_undesirableFields.add("049");
        m_undesirableFields.add("061");
        m_undesirableFields.add("066");
        m_undesirableFields.add("071");
        m_undesirableFields.add("085");
        for (int i = 89; i <= 99; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        m_undesirableFields.add("270");
        m_undesirableFields.add("307");
        m_undesirableFields.add("355");
        m_undesirableFields.add("357");
        for (int i = 363; i <= 388; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }

        m_undesirableFields.add("514");
        m_undesirableFields.add("526");
        m_undesirableFields.add("542");
        m_undesirableFields.add("552");
        m_undesirableFields.add("565");
        m_undesirableFields.add("567");
        m_undesirableFields.add("584");
        m_undesirableFields.add("654");
        for (int i = 656; i <= 658; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        m_undesirableFields.add("662");
        for (int i = 863; i <= 878; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }

        // Unhandled fields
        m_undesirableFields.add("009");
        m_undesirableFields.add("011");
        m_undesirableFields.add("012");
        m_undesirableFields.add("014");
        m_undesirableFields.add("019");
        m_undesirableFields.add("021");
        m_undesirableFields.add("023");
        m_undesirableFields.add("029");
        m_undesirableFields.add("039");
        m_undesirableFields.add("053");
        m_undesirableFields.add("054");
        for (int i = 56; i <= 59; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 62; i <= 65; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 67; i <= 69; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        m_undesirableFields.add("073");
        for (int i = 75; i <= 79; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        m_undesirableFields.add("081");
        m_undesirableFields.add("087");
        for (int i = 189; i <= 199; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 289; i <= 299; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 389; i <= 399; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 491; i <= 499; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 589; i <= 598; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 689; i <= 699; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        m_undesirableFields.add("758");
        for (int i = 831; i <= 855; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 882; i <= 885; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }
        for (int i = 887; i <= 899; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }

        // Local fields
        for (int i = 900; i <= 999; ++i) {
            m_undesirableFields.add(String.format("%03d", i));
        }

        // Handle authority records separately since we don't want 370 in bib
        m_undesirableFieldsAuth = new HashSet<>(m_undesirableFields);
        m_undesirableFieldsAuth.removeAll(Arrays.asList("370", "372", "374", "377"));

        log.info("Started ...");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Map<String, Object> userInfo = (Map<String, Object>) request.getAttribute("user");
        if (userInfo == null) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        log.info("Performing remote search ...");
        String query = request.getParameter("q");
        String startParam = request.getParameter("start");
        int start = (startParam != null ? startParam : "0").equals("0") ? 0 : Integer.parseInt(startParam);
        String nParam = request.getParameter("n");
        int n = (nParam != null ? nParam : "10").equals("10") ? 10 : Integer.parseInt(nParam);
        String databasesParam = request.getParameter("databases");
        List<String> databaseList = Arrays.asList((databasesParam != null ? databasesParam : DEFAULT_DATABASE).split(","));
        String queryStr = null;
        URL url;
        String output = "";

        Map<String, Object> urlParams = new HashMap<>();
        urlParams.put("version", "1.1");
        urlParams.put("operation", "searchRetrieve");
        urlParams.put("maximumRecords", n);
        urlParams.put("startRecord", (start < 1 ? 1 : start));

        log.trace("Query is {}", query);
        if (query != null) {
            List<Map<String, Object>> metaProxyInfo = loadMetaProxyInfo(metaProxyInfoUrl);

            Map<String, String> remoteURLs = new HashMap<>();
            for (Map<String, Object> info : metaProxyInfo) {
                String database = (String) info.get("database");
                remoteURLs.put(database, metaProxyBaseUrl + "/" + database);
            }

            Map<String, String> requiresDcIdentifer = new HashMap<>();
            for (Map<String, Object> info : metaProxyInfo) {
                String database = (String) info.get("database");
                requiresDcIdentifer.put(database, (String) info.get("requiresDcIdentifier"));
            }

            // Weed out the unavailable databases
            databaseList = databaseList.stream()
                    .filter(remoteURLs::containsKey)
                    .collect(Collectors.toList());
            log.debug("Remaining databases: {}", databaseList);
            StringBuilder queryStrBuilder = new StringBuilder();
            for (Map.Entry<String, Object> entry : urlParams.entrySet()) {
                if (queryStrBuilder.length() == 0) {
                    queryStrBuilder.append("?");
                } else {
                    queryStrBuilder.append("&");
                }
                queryStrBuilder.append(entry.getKey()).append("=").append(entry.getValue());
            }
            queryStr = queryStrBuilder.toString();

            if (databaseList.isEmpty()) {
                output = "{\"error\":\"Requested database is unknown or unavailable.\"}";
            }
            else {
                ExecutorService queue = Executors.newCachedThreadPool();
                List<MetaproxySearchResult> resultLists = new ArrayList<>();
                try {
                    List<Future<MetaproxySearchResult>> futures = new ArrayList<>();

                    for (String database : databaseList) {
                        String queryToUse = query;
                        if ("true".equals(requiresDcIdentifer.get(database)) && isValidIsbn(query)) {
                            queryToUse = "dc.identifier=" + query;
                        }

                        url = new URL(remoteURLs.get(database) + queryStr + "&query=" + URLEncoder.encode(queryToUse, StandardCharsets.UTF_8));
                        log.debug("submitting to futures");
                        futures.add(queue.submit(new MetaproxyQuery(url, database)));
                    }
                    log.info("Started all threads. Now harvesting the future");
                    for (Future<MetaproxySearchResult> f : futures) {
                        log.debug("Waiting for future ...");
                        MetaproxySearchResult sr = f.get();
                        log.debug("Adding {} to {}", sr.numberOfHits, resultLists);
                        resultLists.add(sr);
                    }
                } catch (Exception e) {
                    log.error("Error executing futures", e);
                } finally {
                    queue.shutdown();
                }
                // Merge results
                try {
                    output = mergeResults(resultLists);
                } catch (Exception e) {
                    log.error("Error merging results", e);
                    output = "{\"error\":\"Error merging results: " + e.getMessage() + "\"}";
                }
            }
        } else if (request.getParameter("databases") != null) {
            List<Map<String, Object>> databases = loadMetaProxyInfo(metaProxyInfoUrl);
            output = mapper.writeValueAsString(databases);
        } else {
            output = "{\"error\":\"Use parameter 'q'\"}";
        }
        if (output == null || output.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
        } else {
            HttpTools.sendResponse(response, output, "application/json");
        }
    }

    String mergeResults(List<MetaproxySearchResult> resultsList) throws Exception {
        Map<String, Object> results = new LinkedHashMap<>();
        Map<String, Integer> totalResults = new LinkedHashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();
        Map<String, String> errors = new LinkedHashMap<>();

        for (int index : getRange(resultsList)) {
            for (MetaproxySearchResult result : resultsList) {
                if (result.error == null && index < result.hits.size()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("database", result.database);
                    item.put("data", result.hits.get(index).data);
                    items.add(item);
                }
            }
        }

        for (MetaproxySearchResult result : resultsList) {
            totalResults.put(result.database, result.numberOfHits);
            if (result.error != null) {
                errors.put(result.database, result.error);
            }
        }

        results.put("totalResults", totalResults);
        results.put("items", items);
        if (!errors.isEmpty()) {
            results.put("errors", errors);
        }

        return mapper.writeValueAsString(results);
    }

    private List<Integer> getRange(List<MetaproxySearchResult> resultsList) {
        int largestNumberOfHits = resultsList.stream()
                .map(r -> r.hits.size())
                .max(Integer::compareTo)
                .orElse(0);
        return IntStream.range(0, largestNumberOfHits).boxed().collect(Collectors.toList());
    }

    List<Map<String, Object>> loadMetaProxyInfo(URL url) {
        List<Map<String, Object>> databases = new ArrayList<>();
        try {
            GPathResult xml = new XmlSlurper(false, false).parse(url.openStream());

            Object libraryCodesObj = xml.getProperty("libraryCode");
            if (libraryCodesObj instanceof Iterable) {
                for (Object libCode : (Iterable<?>) libraryCodesObj) {
                    GPathResult it = (GPathResult) libCode;
                    Map<String, Object> map = new HashMap<>();
                    map.put("database", createString((GPathResult) it.getProperty("@id")));

                    Object childrenObj = it.children();
                    if (childrenObj instanceof Iterable) {
                        for (Object childNode : (Iterable<?>) childrenObj) {
                            GPathResult node = (GPathResult) childNode;
                            String name = node.name();
                            String text = node.text();
                            Object v = map.get(name);
                            if (v != null) {
                                if (v instanceof String) {
                                    List<String> list = new ArrayList<>();
                                    list.add((String) v);
                                    v = list;
                                    map.put(name, v);
                                }
                                if (v instanceof List) {
                                    ((List<String>) v).add(text);
                                }
                            } else {
                                map.put(name, text);
                            }
                        }
                    }
                    databases.add(map);
                }
            }
        } catch (SocketException se) {
            log.error("Unable to load database list.");
        } catch (Exception e) {
            log.error("Error loading metaproxy info", e);
        }
        return databases;
    }

    class MetaproxyQuery implements Callable<MetaproxySearchResult> {

        URL url;
        String database;

        MetaproxyQuery(URL queryUrl, String db) {
            this.url = queryUrl;
            this.database = db;
        }

        @Override
        public MetaproxySearchResult call() {
            List<String> docStrings = null;
            MetaproxySearchResult results;
            try {
                log.debug("requesting data from url: {}", url);
                String xmlText = new Scanner(url.openStream(), StandardCharsets.UTF_8).useDelimiter("\\A").next();
                GPathResult xmlRecords = new XmlSlurper().parseText(xmlText);
                Map<String, String> namespaces = new HashMap<>();
                namespaces.put("zs", "http://www.loc.gov/zing/srw/");
                namespaces.put("tag0", "http://www.loc.gov/MARC21/slim");
                namespaces.put("diag", "http://www.loc.gov/zing/srw/diagnostic/");
                xmlRecords.declareNamespace(namespaces);

                String errorMessage = null;
                int numHits = 0;
                try {
                    Object numRecords = xmlRecords.getProperty("zs:numberOfRecords");
                    numHits = Integer.parseInt(numRecords.toString());
                } catch (NumberFormatException nfe) {
                    try {
                        GPathResult emessageElement = (GPathResult) ((GPathResult) ((GPathResult) xmlRecords.getProperty("zs:diagnostics"))
                                .getProperty("diag:diagnostic"))
                                .getProperty("diag:message");
                        errorMessage = emessageElement.text();
                    } catch (Exception e) {
                        errorMessage = "Failed to parse error message";
                    }
                } catch (Throwable e) {
                    errorMessage = "zs:numberOfRecords appears to be a bad integer: " + xmlRecords.getProperty("zs:numberOfRecords");
                }
                if (errorMessage == null) {
                    docStrings = getXMLRecordStrings(xmlRecords);
                    results = new MetaproxySearchResult(database, numHits);
                    for (String docString : docStrings) {
                        MarcRecord record = MarcXmlRecordReader.fromXml(docString);

                        String generatedId = sanitizeMarcAndGenerateNewID(record);

                        log.trace("Marcxmlrecordreader for done");
                        String jsonRec = MarcJSONConverter.toJSONString(record);
                        log.trace("Marcjsonconverter for done");
                        Map<String, Object> jsonDoc = marcFrameConverter.convert(mapper.readValue(jsonRec.getBytes(StandardCharsets.UTF_8), Map.class), generatedId);
                        log.trace("Marcframeconverter done");

                        Document doc = new Document(jsonDoc);
                        linkFinder.normalizeIdentifiers(doc);
                        whelk.normalize(doc);
                        whelk.embellish(doc);
                        results.addHit(doc);
                    }
                } else {
                    log.warn("Received errorMessage from metaproxy: {}", errorMessage);
                    results = new MetaproxySearchResult(database, 0);
                    results.error = errorMessage;
                }
            } catch (org.xml.sax.SAXParseException spe) {
                log.error("Failed to parse XML from URL: {}", url);
                results = new MetaproxySearchResult(database, 0);
                results.error = "Failed to parse XML (" + spe.getMessage() + ")";
            } catch (ConnectException ce) {
                log.error("Connection failed", ce);
                results = new MetaproxySearchResult(database, 0);
                results.error = "Connection with metaproxy failed.";
            } catch (Exception e) {
                log.error("Could not convert document", e);
                results = new MetaproxySearchResult(database, 0);
                results.error = "Failed to convert results to JSON.";
            }
            return results;
        }
    }

    class MetaproxySearchResult {

        List<Document> hits = new ArrayList<>();
        String database, error;
        int numberOfHits;

        MetaproxySearchResult(String db, int nrHits) {
            this.numberOfHits = nrHits;
            this.database = db;
        }

        void addHit(Document doc) {
            hits.add(doc);
        }
    }

    List<String> getXMLRecordStrings(GPathResult xmlRecords) {
        List<String> xmlRecs = new ArrayList<>();
        try {
            Object recordsObj = xmlRecords.getProperty("zs:records");
            if (recordsObj != null) {
                Object allRecords = ((GPathResult) recordsObj).getProperty("zs:record");
                if (allRecords instanceof Iterable) {
                    for (Object record : (Iterable<?>) allRecords) {
                        GPathResult recordData = (GPathResult) ((GPathResult) record).getProperty("zs:recordData");
                        String marcXmlRecord = createString((GPathResult) recordData.getProperty("tag0:record"));
                        if (marcXmlRecord != null && !marcXmlRecord.isEmpty()) {
                            xmlRecs.add(removeNamespacePrefixes(marcXmlRecord));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error extracting XML record strings", e);
        }
        return xmlRecs;
    }

    String createString(GPathResult root) {
        StreamingMarkupBuilder builder = new StreamingMarkupBuilder();
        return builder.bind(root).toString();
    }

    private boolean isValidIsbn(String query) {
        try {
            return IsbnParser.parse(query.trim()) != null;
        } catch (Exception e) {
            return false;
        }
    }

    String removeNamespacePrefixes(String xmlStr) {
        return xmlStr.replaceAll("tag0:", "").replaceAll("zs:", "");
    }

    String sanitizeMarcAndGenerateNewID(MarcRecord marcRecord) {
        List<Field> mutableFieldList = marcRecord.getFields();

        String generatedId = IdGenerator.generate();
        LegacyIntegrationTools.makeRecordLibrisResident(marcRecord);
        if (marcRecord.getControlfields("001").size() != 0) {
            mutableFieldList.remove(marcRecord.getControlfields("001").get(0));
        }
        marcRecord.addField(marcRecord.createControlfield("001", generatedId));

        // Remove unwanted marc fields.
        boolean isAuth = marcRecord.getLeader().charAt(6) == 'z';
        Set<String> undesirableFields = isAuth ? m_undesirableFieldsAuth : m_undesirableFields;
        Iterator<Field> it = mutableFieldList.iterator();
        while (it.hasNext()) {
            Field field = it.next();
            String fieldNumber = field.getTag();
            if (undesirableFields.contains(fieldNumber)) {
                it.remove();
            }
        }
        return generatedId;
    }
}
