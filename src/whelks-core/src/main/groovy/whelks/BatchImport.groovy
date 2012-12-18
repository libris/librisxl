package se.kb.libris.whelks.imports;

import groovy.util.logging.Slf4j as Log
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

import groovy.transform.Synchronized

import java.util.concurrent.*
import java.util.concurrent.atomic.*
import java.util.Calendar
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.text.*
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import se.kb.libris.conch.converter.MarcJSONConverter
import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

@Log
class BatchImport {

    private String resource;

    private long starttime = 0;
    private int NUMBER_OF_IMPORTERS = 20
    def pool
    int importedDocs = 0

    public BatchImport() {}


    public BatchImport(String resource) {
        this.resource = resource;
    }

    URL getBaseUrl(Date from, Date until) {
        String url = "http://data.libris.kb.se/"+this.resource+"/oaipmh/?verb=ListRecords&metadataPrefix=marcxml"
        if (from != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
            url = url + "&from=" + sdf.format(from)
            if (until) {
                url = url + "&until=" + sdf.format(until)
                log.debug("" + sdf.format(from) + "-" + sdf.format(until))
            }
        }
        log.debug("URL: $url")
        return new URL(url)
    }

    public void setResource(String r) { this.resource = r }

    public int doImport(Whelk whelk, Date from, Integer noOfDocs) {
        try {
            pool = Executors.newCachedThreadPool()

            this.starttime = System.currentTimeMillis()
            List<Future> futures = []
            if (from && noOfDocs) {
                def dateFormatter = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss")
                futures << pool.submit(new Harvester(whelk, this.resource, getBaseUrl(from, null), dateFormatter.format(from) + " and docs count $noOfDocs", noOfDocs.intValue()))
            } else { 
                futures << pool.submit(new Harvester(whelk, this.resource, getBaseUrl(from, null), "alla", null))
            }
            def results = futures.collect{it.get()}
            log.info("Collecting results ...")
            log.info("Results: $results, after " + (System.currentTimeMillis() - starttime)/1000 + " seconds.")
        } finally {
            pool.shutdown()
        }
        return 0
    }

    Date getYearDate(int year) {
        def sdf = new SimpleDateFormat("yyyy")
        return sdf.parse("" + year)
    }
}

@Log
class Harvester implements Runnable {
    URL url
    Whelk whelk
    String resource
    private final AtomicInteger importedCount = new AtomicInteger()
    private int failed = 0
    private int xmlfailed = 0
    String year
    def storepool
    def queue
    def executor
    static final int CORE_POOL_SIZE = 500
    static final int MAX_POOL_SIZE = 500
    static final long KEEP_ALIVE_TIME = 60
    final long startTime
    private int harvestMax = 100000000
    private int harvestCount = 0
    private String lastTimestamp

    Harvester(Whelk w, String r, URL u, String y, Integer harvestMax) {
        this.url = new URL(u.toString())
        this.resource = r
        this.whelk = w
        this.year = y
        this.startTime = System.currentTimeMillis()
        if (harvestMax) this.harvestMax = harvestMax.intValue()
        executor = newScalingThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME)
    }

    private void getAuthentication() {
        try {
            Properties properties = new Properties()
            properties.load(this.getClass().getClassLoader().getResourceAsStream("whelks-core.properties"))
            final String username = properties.getProperty("username")
            final String password = properties.getProperty("password")
            Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray())
                    }
                });
        } catch (Exception ex) {
            Logger.getLogger(BatchImport.class.getName()).log(Level.SEVERE, null, ex)
        }
    }

    public ThreadPoolExecutor newScalingThreadPoolExecutor(int min, int max, long keepAliveTime) {
        ScalingQueue queue = new ScalingQueue()
        ThreadPoolExecutor executor = new ScalingThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.SECONDS, queue)
        executor.setRejectedExecutionHandler(new ForceQueuePolicy())
        queue.setThreadPoolExecutor(executor)
        return executor
    }

    public void run() {
        try {
            Thread monitorThread = new Thread(new MonitorThread(executor, year))
            monitorThread.setDaemon(true)
            monitorThread.start()
            log.info("Staring monitor thread for harvester " + year)
            getAuthentication();
            log.info("Starting harvester with url: $url")
            String resumptionToken = harvest(this.url)
            harvestCount = harvestCount + 100
            while (resumptionToken != null && harvestCount < harvestMax) {
                URL rurl = new URL("http://data.libris.kb.se/" + this.resource + "/oaipmh/?verb=ListRecords&resumptionToken=" + resumptionToken)
                resumptionToken = harvest(rurl)
                log.trace("Received resumptionToken $resumptionToken")
                harvestCount = harvestCount + 100
            }
        } finally {
            log.info("Harvester for ${this.year} has ended its run. $importedCount documents imported. $failed failed. $xmlfailed failed XML documents. Last timestamp: $lastTimestamp")
            File logFile = new File("import.log")
            logFile.write("Harvester for ${this.year}. $importedCount documents imported. $failed failed. $xmlfailed failed XML documents. Last timestamp: $lastTimestamp")
            this.executor.shutdown()
            if (!executor.awaitTermination(KEEP_ALIVE_TIME, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    String harvest(URL url) {
        String mdrecord = null
        String xmlString
        def OAIPMH
        def documents = []
        try {
            log.trace("URL.text: ${url.text}")
            xmlString = normalizeString(url.text)
            OAIPMH = new XmlSlurper(false,false).parseText(xmlString)
            if (OAIPMH.ListRecords.size() == 0) {
                log.info("No records found in $xmlString. Returning null for $url")
                return null
            }
            OAIPMH.ListRecords.record.each {
                mdrecord = createString(it.metadata.record)
                if (mdrecord) {
                    MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)
                    String id = record.getControlfields("001").get(0).getData()
                    String jsonRec = MarcJSONConverter.toJSONString(record)
                    def doc = new BasicDocument().withData(jsonRec.getBytes("UTF-8")).withIdentifier("/" + whelk.prefix + "/" + id).withContentType("application/json")
                    if (it.header.setSpec) {
                        for (sS in it.header.setSpec) {
                            if (sS.toString().startsWith("authority:")) {
                                def auth = "/auth/" + sS.toString().substring(10)
                                doc = doc.withLink(auth, "authority")
                            }
                            if (sS.toString().startsWith("location:")) {
                                doc = doc.tag("location", sS.toString().substring(9))
                            }
                        }
                    }
                    if (it.header.datestamp) {
                        lastTimestamp = it.header.datestamp.toString()
                    }
                    documents << doc
                    //log.debug("Imported " + importedCount.incrementAndGet())
                } else {
                    failed++
                }
            }

            if (!OAIPMH.ListRecords.resumptionToken) {
                throw new WhelkRuntimeException("No res-token found in $xmlString : " + OAIPMH.ListRecords.resumptionToken)
            }
            return OAIPMH.ListRecords.resumptionToken
        } catch (java.io.IOException ioe) {
            log.warn("Failed to parse record \"$mdrecord\": ${ioe.message}.")
            return OAIPMH.ListRecords.resumptionToken
        } catch (Exception e) {
            log.warn("Failed to parse XML document \"${xmlString}\": ${e.message}. Trying to extract resumptionToken and continue. ($url)")
            log.warn(e.printStackTrace())
            xmlfailed++
            return findResumptionToken(xmlString)
        } finally {
            if (documents.size() > 0) {
                executor.execute(new Runnable() {
                        public void run() {
                            importedCount.addAndGet(documents.size())
                            float elapsed = ((System.currentTimeMillis() - startTime) / 1000)
                            log.info("Storing " + documents.size() + ". Imported $importedCount documents in $elapsed seconds sofar. That's " + importedCount/elapsed + " docs per sec.")
                            whelk.store(documents)
                        }
                    })
            } else log.debug("Harvest on $url resulted in no documents. xmlstring: ${xmlString}")
        }
    }

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    String findResumptionToken(xmlString) {
        log.info("findResumption ...")
        try {
            String rt = xmlString.split("(<)/?(resumptionToken>)")[1]
            log.info("Found $rt")
            return rt
        } catch (ArrayIndexOutOfBoundsException a) {
            log.warn("Failed to extract resumptionToken from xml:\n$xmlString")
        }
        return null
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }
}

@Log
class ScalingQueue extends LinkedBlockingQueue {
    private ThreadPoolExecutor executor

    public ScalingQueue() { super() }
    public ScalingQueue(int capacity) { super(capacity) }

    public setThreadPoolExecutor(ThreadPoolExecutor executor) {
        this.executor = executor
    }

    @Override
    public boolean offer(Object o) {
        int allWorkingThreads = executor.getActiveCount()
        return allWorkingThreads < executor.getPoolSize() && super.offer(o)
    }
}

@Log
class ForceQueuePolicy implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        try {
            executor.getQueue().put(r)
        } catch(InterruptedException ie) {
            throw new Exception(ie)
        }
    }
}

@Log
class ScalingThreadPoolExecutor extends ThreadPoolExecutor {
    private final AtomicInteger activeCount = new AtomicInteger()

    public ScalingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, LinkedBlockingQueue queue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, queue)
    }

    @Override
    public int getActiveCount() {
        return activeCount.get()
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        activeCount.incrementAndGet()
        if (getActiveCount() == getCorePoolSize()) {
            Thread.sleep(1000)
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        activeCount.decrementAndGet()
    }
}

@Log
class MonitorThread implements Runnable {
    private ThreadPoolExecutor executor
    private String harvester

    public MonitorThread(ThreadPoolExecutor executor, String harvester) {
        this.executor = executor
        this.harvester = harvester
    }

    public void run() {
        while(true) {
            log.debug("Harvester: " + harvester + ". Number of executor active threads " + executor.getActiveCount() + ". Task queue size " + executor.getQueue().size() + ". Completed tasks so far " + executor.getCompletedTaskCount())
            Thread.sleep(5000)
        }
    }
}
