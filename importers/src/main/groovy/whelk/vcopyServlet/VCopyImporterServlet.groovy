package whelk.vcopyServlet

import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.filter.LinkFinder
import whelk.importer.BrokenRecordException
import whelk.importer.ImportResult
import whelk.importer.VCopyImporter
import whelk.util.PropertyLoader

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary

/**
 * Copy of OAIPMH Harvester servlet, but using vcopy as data source.
 */
@Log
@Deprecated
class VCopyImporterServlet extends HttpServlet {

    int scheduleDelaySeconds = 5
    Properties props = new Properties()
    private Map<String, ScheduledJob> jobs = [:]
    private Whelk whelk

    static String SETTINGS_PFX = "harvester:"
    static String DEFAULT_IMPORTER = "whelk.importer.VCopyImporter"
    static int DEFAULT_INTERVAL = 60
    static String DEFAULT_SYSTEM = "vcopy"
    static List<String> DEFAULT_SERVICES = [SETTINGS_PFX + 'auth', SETTINGS_PFX + 'bib', SETTINGS_PFX + 'hold']

    static final ObjectMapper mapper = new ObjectMapper()

    static final Counter requests = Counter.build()
            .name("vcopy_importer_requests_total").help("Total requests to VCopy Importer.")
            .labelNames("method").register()

    static final Counter failedRequests = Counter.build()
            .name("vcopy_importer_failed_requests_total").help("Total failed requests to VCopy Importer.")
            .labelNames("method", "resource", "status").register()

    static final Gauge ongoingRequests = Gauge.build()
            .name("vcopy_importer_ongoing_requests_total").help("Total ongoing VCopy Importer requests.")
            .labelNames("method").register()

    static final Summary requestsLatency = Summary.build()
            .name("vcopy_importer_requests_latency_seconds")
            .help("VCopy Importer request latency in seconds.")
            .labelNames("method").register()


    VCopyImporterServlet() {
        log.info("Starting vcopyImporter.")

        props = PropertyLoader.loadProperties('secret', 'mysql')
        PostgreSQLComponent pg = new PostgreSQLComponent(props)
        whelk = new Whelk(pg)
        log.info("Started ...")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("GET").inc()
        ongoingRequests.labels("GET").inc()
        Summary.Timer requestTimer = requestsLatency.labels("GET").startTimer()
        log.debug("Handling GET request.")
        try {
            doGet2(response, request)
        } finally {
            ongoingRequests.labels("GET").dec()
            requestTimer.observeDuration()
        }
    }

    private void doGet2(HttpServletResponse response, HttpServletRequest request) {
        String html, json
        if (jobs) {
            List<String> services = DEFAULT_SERVICES
            Map state = [:]
            StringBuilder table = new StringBuilder("<table cellspacing=\"10\"><tr><th>&nbsp;</th>")
            table.append("<form method=\"post\">")

            Set catSet = new TreeSet<String>()

            for (service in services) {
                state[service] = whelk.storage.loadSettings(service)
                state[service]["harvesterClass"] = DEFAULT_IMPORTER
                state[service]["interval"] = DEFAULT_INTERVAL
                catSet.add("harvesterClass")
                catSet.add("interval")
                catSet.addAll(state[service].keySet())
                table.append("<th>$service</th>")

            }
            table.append("</tr>")
            List categories = catSet.toList()

            for (cat in categories) {
                table.append("<tr><td align=\"right\"><b>$cat</b></td>")
                for (collection in services) {
                    table.append("<td>${state.get(collection).get(cat) != null ? state.get(collection).get(cat) : "&nbsp;"}</td>")
                }
                table.append("</tr>")
            }
            table.append("<tr><td><input type=\"submit\" name=\"action_all\" value=\"stop all\"></td>")
            for (collection in services) {
                table.append("<td><input type=\"submit\" name=\"action_${collection}\" value=\"${jobs[collection]?.active ? "stop" : "start"}\">")
                if (jobs[collection] && !jobs[collection].active) {
                    String lastImportDate = jobs[collection].getLastImportValue().format("yyyy-MM-dd'T'HH:mm")
                    table.append("&nbsp;<input type=\"submit\" name=\"reset_${collection}\" value=\"reload $collection from\"/>&nbsp;<input type=\"datetime-local\" name=\"datevalue\" value=\"${lastImportDate}\"/>")
                }
                table.append("</td>")
            }
            table.append("</tr>")

            table.append("</form></table>")

            html = """
                <html><head><title>OAIPMH Harvester control panel</title></head>
                <body>
                ${table.toString()}
                </form>
                """
            json = mapper.writeValueAsString(state)
        } else {

            html = """
                <html><head><title>OAIPMH Harvest Disabled</title></head>
                <body>

                HARVESTER DISABLED<br/>
                """
            json = mapper.writeValueAsString(["state": "disabled"])
        }
        PrintWriter out = response.getWriter();

        if (request.getPathInfo() == "/json") {
            response.setContentType("application/json");
            out.print(json);
        } else {
            response.setContentType("text/html");
            out.print(html);
        }

        out.flush();
    }

    void doPost(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("POST").inc()
        ongoingRequests.labels("POST").inc()
        Summary.Timer requestTimer = requestsLatency.labels("POST").startTimer()
        log.debug("Received post request. Got this: ${request.getParameterMap()}")
        try {
            doPost2(request, response)
        } finally {
            ongoingRequests.labels("POST").dec()
            requestTimer.observeDuration()
        }
    }

    private void doPost2(HttpServletRequest request, HttpServletResponse response) {
        for (reqs in request.getParameterNames()) {
            if (reqs == "action_all") {
                for (job in jobs) {
                    job.value.disable()
                }
            } else if (reqs.startsWith("reset_")) {
                log.debug("Loading job for ${reqs.substring(6)}")
                log.debug("Got these jobs: $jobs")
                def job = jobs.get(reqs.substring(6))
                Date startDate = Date.parse("yyyy-MM-dd'T'HH:mm", request.getParameter("datevalue"))
                log.debug("Resetting harvester for ${job.collection} to $startDate")
                job.setStartDate(startDate)
                //job.enable()
            } else if (reqs.startsWith("action_")) {
                jobs.get(reqs.substring(7)).toggleActive()
            }
        }
        response.sendRedirect(request.getRequestURL().toString())
    }

    void init() {

        log.debug "Props: ${props.inspect()}"
        log.info("Initializing vcopy importer.")
        List<String> services = DEFAULT_SERVICES

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(services.size())

        for (service in services) {
            log.info("Setting up schedule for $service")
            String vcopyConnectionString = props.getProperty("mysqlConnectionUrl")
            int scheduleIntervalSeconds = DEFAULT_INTERVAL as int
            String sourceSystem = DEFAULT_SYSTEM
            def job = new ScheduledJob(whelk, new VCopyImporter(whelk),
                    "${service}",
                    sourceSystem,
                    vcopyConnectionString)
            jobs[service] = job

            try {
                ses.scheduleWithFixedDelay(job, scheduleDelaySeconds, scheduleIntervalSeconds, TimeUnit.SECONDS)
            } catch (RejectedExecutionException ree) {
                log.error("execution failed", ree)
            }
        }
        log.info("scheduler started")
    }
}

@Log
class ScheduledJob implements Runnable {

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

    String collection, sourceSystem
    Whelk whelk
    VCopyImporter importer
    String vcopyConnectionString
    Map whelkState = null
    boolean active = true
    final static long WEEK_MILLIS = 604800000

    ScheduledJob(Whelk whelk, VCopyImporter importer, String coll, String sSystem, String conStr) {
        this.vcopyConnectionString = conStr
        this.importer = importer
        this.whelk = whelk
        this.collection = coll
        this.sourceSystem = sSystem
        assert whelk.storage
        assert collection
        loadWhelkState().remove("lastError")
        loadWhelkState().remove("lastErrorDate")
    }

    void toggleActive() {
        active = !active
        if (active) {
            loadWhelkState().remove("lastError")
            loadWhelkState().remove("lastErrorDate")
        }
        loadWhelkState().put("status", (active ? "IDLE" : "STOPPED"))
        whelk.storage.saveSettings(collection, whelkState)
    }

    void disable() {
        active = false
        loadWhelkState().put("status", "STOPPED")
        whelk.storage.saveSettings(collection, whelkState)
    }

    void enable() {
        active = true
        loadWhelkState().put("status", "IDLE")
        whelk.storage.saveSettings(collection, whelkState)
    }

    void setStartDate(Date startDate) {
        loadWhelkState().put("lastImport", startDate.format(DATE_FORMAT))
        whelk.storage.saveSettings(collection, whelkState)
    }

    Date getLastImportValue() {
        String dateString = loadWhelkState().get("lastImport")
        if (!dateString) {
            return new Date(new Date().getTime() - WEEK_MILLIS)
        } else {
            return Date.parse(DATE_FORMAT, dateString)
        }
    }

    Map loadWhelkState() {
        if (!whelkState) {
            log.debug("Loading current state from storage ...")
            whelkState = whelk.storage.loadSettings(collection)
            log.debug("Loaded state for $collection : $whelkState")
        }
        return whelkState
    }

    void run() {
        if (active) {
            loadWhelkState()

            log.debug("Current whelkstate: $whelkState")
            try {
                String lastImport = whelkState.get("lastImport")
                Date currentSince
                Date nextSince = new Date()
                if (lastImport) {
                    log.trace("Parsing $lastImport as date")
                    currentSince = Date.parse(DATE_FORMAT, lastImport)
                    nextSince = new Date(currentSince.getTime() + 1000)
                    log.trace("Next since (upped by 1 second): $nextSince")
                } else {
                    def lastWeeksDate = nextSince[Calendar.DATE] - 2//7
                    nextSince.set(date: lastWeeksDate)
                    currentSince = nextSince
                    log.info("Importer has no state for last import from $collection. Setting last week (${nextSince})")
                }
                if (nextSince.after(new Date())) {
                    log.warn("Since is slipping ... Is now ${nextSince}. Resetting to one second ago.")
                    nextSince = new Date(new Date().getTime() - 1000)
                }
                log.debug "Executing vcopy harvest import for ${collection} since ${nextSince}"
                whelkState.put("status", "RUNNING")

                whelk.storage.saveSettings(collection, whelkState)
                ImportResult result = importer.doImport(collection.replace(VCopyImporterServlet.SETTINGS_PFX, ''), sourceSystem, vcopyConnectionString, nextSince)
                log.trace("Import completed, result: $result")
                if (result && (result.numberOfDocuments > 0 || result.numberOfDocumentsDeleted > 0 || result.numberOfDocumentsSkipped > 0)) {
                    log.debug("Imported ${result.numberOfDocuments} documents and deleted ${result.numberOfDocumentsDeleted} for $collection. Last record has datestamp: ${result.lastRecordDatestamp.format(DATE_FORMAT)}")
                    whelkState.put("lastImportNrImported", result.numberOfDocuments)
                    whelkState.put("lastImportNrDeleted", result.numberOfDocumentsDeleted)
                    whelkState.put("lastImportNrSkipped", result.numberOfDocumentsSkipped)
                    whelkState.put("lastImport", result.lastRecordDatestamp.format(DATE_FORMAT))

                } else {
                    log.debug("Imported ${result.numberOfDocuments} document for $collection.")
                    whelkState.put("lastImport", currentSince.format(DATE_FORMAT))
                }
                whelkState.put("status", "IDLE")
                whelkState.put("lastRunNrImported", result.numberOfDocuments)
                whelkState.put("lastRun", new Date().format(DATE_FORMAT))
            } catch (BrokenRecordException bre) {
                whelkState.get("badRecords", []).add(bre.brokenId)
                whelkState.put("status", "ERROR")
            } catch (IOException ioe) {
                whelkState.put("lastError", new String("Failed to connect. Reason: ${ioe.message} (${ioe.getClass().getName()})"))
                whelkState.put("lastErrorDate", new Date().toString())
                whelkState.put("status", "ERROR")
            } catch (Exception e) {
                log.error("Something failed: ${e.message}", e)
            } finally {
                log.debug("Saving state $whelkState")
                whelk.storage.saveSettings(collection, whelkState)
            }
        }
    }

}

