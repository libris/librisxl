package whelk.importer.xinfo

import groovy.util.logging.Slf4j as Log
import groovy.util.slurpersupport.GPathResult
import groovy.xml.StreamingMarkupBuilder
import se.kb.libris.util.marc.MarcRecord
import whelk.Document
import whelk.importer.MySQLImporter

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder

import java.text.Normalizer;
import java.util.concurrent.*

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

@Log
class XInfoImporter extends MySQLImporter {

    int sqlLimit = 1000

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"

    boolean cancelled = false

    ExecutorService queue

    int startAt = 0

    int recordCount
    long startTime
    HttpClient client = HttpClientBuilder.create().build();

    PreparedStatement prepareStatement(String dataset, Connection conn) {
        return conn.prepareStatement("SELECT * FROM xinfo.resource LIMIT ?, $sqlLimit")
    }

    int doImport(String dataset, int nrOfDocs = -1, boolean silent = false, boolean picky = true, URI serviceUrl = null) {
        recordCount = 0
        startTime = System.currentTimeMillis()
        cancelled = false
        Connection conn = null
        PreparedStatement statement = null
        ResultSet resultSet = null

        if (nrOfDocs > 0 && nrOfDocs < sqlLimit) { sqlLimit = nrOfDocs }

        queue = Executors.newWorkStealingPool()

        def versioningSettings = [:]

        //log.info("Suspending camel during import.")
        //whelk.camelContext.suspend()
        for (st in this.whelk.getStorages()) {
            log.debug("Turning off versioning in ${st.id}")
            // Preserve original setting
            versioningSettings.put(st.id, st.versioning)
            st.versioning = false
        }

        try {

            Class.forName("com.mysql.jdbc.Driver")

            log.debug("Connecting to database...")
            conn = connectToUri(serviceUrl)

            statement = prepareStatement(dataset, conn)

            if (!statement) {
                throw new Exception("Failed to create a prepared statement")
            }

            log.info("Starting loading at ID $startAt")

            for (;;) {
                statement.setInt(1, startAt)
                log.debug("Reset statement with $startAt")
                resultSet = statement.executeQuery()

                log.debug("Query executed. Starting processing ...")
                def imgdocs = []
                def jsondocs = [:]
                while(resultSet.next()) {
                    String xinfoId = resultSet.getString("id")
                    String xinfoUrl = resultSet.getString("url")
                    String type = resultSet.getString("type")
                    String whelkId = createWhelkId(xinfoId, type)
                    if (type == "PICTURE" && xinfoUrl.startsWith("/")) {
                        log.debug("Loading all versions of image for $xinfoUrl, saving to base $whelkId")
                        Document doc
                        if (jsondocs.containsKey(whelkId)) {
                            doc = jsondocs.get(whelkId)
                        } else {
                            doc = whelk.get(whelkId)
                        }
                        if (!doc) {
                            doc = whelk.createDocument("application/ld+json").withIdentifier(whelkId).withDataset("xinfo").withData('{"@type":"xinfo"}')
                        }
                        jsondocs.put(doc.identifier, doc)
                        ["orginal", "hitlist", "record"].each {
                            String version = it
                            byte[] imgBytes = new URL("http://xinfo.libris.kb.se" + xinfoUrl + "/"+ version).getBytes()
                            if (imgBytes.length > 0) {
                                if (version == "orginal") {
                                    version = "original"
                                }
                                imgdocs << whelk.createDocument("image/jpeg").withIdentifier(whelkId + "/image/" + version).withData(imgBytes).withDataset("image")
                            }
                        }
                    } else if (xinfoUrl.startsWith("/")) {
                        Document doc
                        if (jsondocs.containsKey(whelkId)) {
                            doc = jsondocs.get(whelkId)
                        } else {
                            doc = whelk.get(whelkId)
                        }
                        if (!doc) {
                            doc = whelk.createDocument("application/ld+json").withIdentifier(whelkId).withDataset("xinfo")
                        }
                        def dataMap = doc.dataAsMap
                        dataMap.put("@type", "xinfo")
                        dataMap.put(type.toLowerCase(), loadXinfoText(xinfoUrl))
                        doc.withData(dataMap)
                        jsondocs.put(doc.identifier, doc)
                    }
                    recordCount++
                    startAt++
                }
                queue.execute({
                    if (imgdocs.size() > 0) {
                        this.whelk.bulkAdd(imgdocs, "image/jpeg")
                    }
                    if (jsondocs.size() > 0) {
                        this.whelk.bulkAdd(jsondocs.values() as List, "application/ld+json")
                    }
                } as Runnable)

                if (nrOfDocs > 0 && recordCount > nrOfDocs) {
                    log.info("Max docs reached. Breaking.")
                    break
                }

                if (cancelled) {
                    log.info("Cancelled. Breaking.")
                    break
                }
            }
            log.debug("Clearing out remaining docs ...")

        } catch(SQLException se) {
            log.error("SQL Exception", se)
        } catch(Exception e) {
            log.error("Exception", e)
        } finally {
            log.info("Record count: ${recordCount}. Elapsed time: " + (System.currentTimeMillis() - startTime) + " milliseconds for sql results.")
            close(conn, statement, resultSet)
        }

        queue.execute({
            this.whelk.flush()
            log.debug("Resetting versioning setting for storages")
            for (st in this.whelk.getStorages()) {
                st.versioning = versioningSettings.get(st.id)
            }
            //log.info("Starting camel context ...")
            //whelk.camelContext.resume()
        } as Runnable)

        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        log.info("Import has completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
        return recordCount
    }

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    String washXmlOfBadCharacters(String xmlString) {
        StringBuilder sb = new StringBuilder(xmlString)
        for (int i=0;i<sb.length();i++)
            if (sb.charAt(i) < 0x09 || (sb.charAt(i) > 0x0D && sb.charAt(i) < 0x1F)) {
                log.warn("Found illegal character: ${sb.charAt(i)}")
                sb.setCharAt(i, '?' as char);
            }

        return sb.toString()
    }

    String loadXinfoText(String url) {
        URL xinfoUrl = new URL("http://xinfo.libris.kb.se" + url + "&type=summary")
        log.debug("Loading xinfo metadata from $xinfoUrl")
        String xmlString = washXmlOfBadCharacters(normalizeString(xinfoUrl.text))
        if (xmlString) {
            //log.info("xmlString: $xmlString")
            try {
                def summary = new XmlSlurper(false,false).parseText(xmlString)
                String infoText = summary.text()
                log.trace("Setting infotext: $infoText")
                return infoText
            } catch (org.xml.sax.SAXParseException spe) {
                log.error("XML contains invalid characters: ${spe.message} :\n$xmlString")
            }
        }
        return null
    }

    String createWhelkId(String xiId, String type) {
        if (xiId.startsWith("libris-bib:")) {
            return "/xinfo/bib/" + xiId.substring(11)
        } else {
            return "/xinfo/" + xiId
        }
        return null
    }

    Connection connectToUri(URI uri) {
        log.info("connect uri: $uri")
        DriverManager.getConnection(uri.toString())
    }

    public void close(Connection conn, PreparedStatement statement, ResultSet resultSet) {
        log.info("Closing down mysql connections.")
        try {
            statement.cancel()
            if (resultSet != null) {
                resultSet.close()
            }
        } catch (SQLException e) {
            log.warn("Exceptions on close. These are safe to ignore.", e)
        } finally {
            try {
                statement.close()
                conn.close()
            } catch (SQLException e) {
                log.warn("Exceptions on close. These are safe to ignore.", e)
            } finally {
                resultSet = null
                statement = null
                conn = null
            }
        }
    }


    @Override
    int getRecordCount() {
        return 0
    }

    @Override
    void cancel() {

    }
}
