package whelk.importer.xinfo

import groovy.util.logging.Slf4j as Log
import groovy.util.slurpersupport.GPathResult
import groovy.xml.StreamingMarkupBuilder
import se.kb.libris.util.marc.MarcRecord
import whelk.Document
import whelk.plugin.Importer
import whelk.plugin.BasicPlugin
import whelk.result.ImportResult

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
class XInfoImporter extends BasicPlugin implements Importer {

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

    ImportResult doImport(String dataset, int nrOfDocs = -1, boolean silent = false, boolean picky = true, URI serviceUrl = null) {
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
                def jsondocs = []
                def coverjsondocs = []
                while(resultSet.next()) {
                    String xinfoId = resultSet.getString("id")
                    String xinfoUrl = resultSet.getString("url")
                    String type = resultSet.getString("type")
                    String supplier = resultSet.getString("supplier")
                    String whelkId = createWhelkId(xinfoId, type)
                    if ((type == "PICTURE" || type == "BILD") && xinfoUrl.startsWith("/")) {
                        log.debug("Loading all versions of image for $xinfoUrl, saving to base $whelkId")
                        Document coverDoc = whelk.createDocument("application/ld+json").withIdentifier(whelkId + "/cover").withDataset("cover").withData('{"@type":"CoverArt"}')
                        def coverDocMap = coverDoc.dataAsMap
                        ["orginal", "hitlist", "record"].each {
                            String version = it
                            String imgUrl = "http://xinfo.libris.kb.se" + xinfoUrl + "/"+ version
                            try {
                                byte[] imgBytes = new URL(imgUrl).getBytes()
                                if (imgBytes.length > 0) {
                                    if (version == "orginal") {
                                        version = "original"
                                    }
                                    imgdocs << whelk.createDocument("image/jpeg").withIdentifier(whelkId + "/cover/" + version).withData(imgBytes).withDataset("image")
                                    if (version == "original") {
                                        coverDocMap.put("covertArt", whelkId + "/cover/" + version)
                                    }
                                    if (version == "hitlist") {
                                        coverDocMap.put("covertArtThumb", whelkId + "/cover/" + version)
                                    }
                                    if (version == "record") {
                                        coverDocMap.put("covertArtMidsize", whelkId + "/cover/" + version)
                                    }
                                    if (whelkId.contains("/bib/")) {
                                        coverDocMap.put("annotates", ["@id": "/resource/" + whelkId.substring(7)])
                                    } else {
                                        coverDocMap.put("annotates", ["@id": "urn:" + whelkId.substring(7)])
                                    }
                                    if (supplier) {
                                        coverDocMap.put("annotationSource", ["name": supplier])
                                    }
                                } else {
                                    log.warn("No data at $imgUrl")
                                }
                            } catch (IOException ioe) {
                                log.warn("Error retrieving data from $imgUrl ... Skipping")
                            }
                        }
                        coverDoc.withData(coverDocMap)
                        coverjsondocs << coverDoc
                    } else if (xinfoUrl.startsWith("/")) {
                        try {
                            Document doc = whelk.createDocument("application/ld+json").withIdentifier(whelkId + "/" + type.toLowerCase()).withDataset("annotation")
                            def dataMap = doc.dataAsMap
                            dataMap.put("@id", whelkId)
                            dataMap.put("text", loadXinfoText(xinfoUrl))
                            if (type == "TOC") {
                                dataMap.put("@type", "TableOfContents")
                            } else {
                                dataMap.put("@type", type.toLowerCase().capitalize())
                            }
                            if (supplier) {
                                dataMap.put("annotationSource", ["name": supplier])
                            }
                            if (whelkId.contains("/bib/")) {
                                dataMap.put("annotates", ["@id": "/resource/" + whelkId.substring(7)])
                            } else {
                                dataMap.put("annotates", ["@id": "urn:" + whelkId.substring(7)])
                            }
                            doc.withData(dataMap)
                            jsondocs << doc
                        } catch (Exception e) {
                            log.error("Failed to create document for $xinfoUrl", e)
                        }
                    }
                    recordCount++
                    startAt++
                }
                queue.execute({
                    if (imgdocs.size() > 0) {
                        this.whelk.bulkAdd(imgdocs, "image", "image/jpeg")
                    }
                    if (coverjsondocs.size() > 0) {
                        this.whelk.bulkAdd(coverjsondocs, "cover", "application/ld+json")
                    }
                    if (jsondocs.size() > 0) {
                        this.whelk.bulkAdd(jsondocs, "annotation", "application/ld+json")
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
        return new ImportResult(numberOfDocuments: recordCount)
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
        return sb.toString().replaceAll("&#(\\d+);", " ")
    }

    String loadXinfoText(String url) {
        URL xinfoUrl = new URL("http://xinfo.libris.kb.se" + url + "&type=summary")
        log.debug("Loading xinfo metadata from $xinfoUrl")
        boolean loaded = false
        String xmlString = null
        while (!loaded) {
            try {
                xmlString = washXmlOfBadCharacters(normalizeString(xinfoUrl.text))
                loaded = true
            } catch (java.net.ConnectException ce) {
                log.info("ConnectException. Failed to load text from ${xinfoUrl}. Trying again in a few seconds.")
                Thread.sleep(5000)
            }
        }
        if (xmlString) {
            try {
                def summary = new XmlSlurper(false,false).parseText(xmlString)
                String infoText = removeMarkup(summary.text())
                log.trace("Setting infotext: $infoText")
                return infoText
            } catch (org.xml.sax.SAXParseException spe) {
                log.error("XML contains invalid characters: ${spe.message} :\n$xmlString")
            }
        }
        return null
    }

    String removeMarkup(String text) {
        return text.replaceAll("<[^>]*>", " ").replaceAll(/\s{2,}/, " ").trim()
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
    void cancel() {
        cancelled = true
    }
}
