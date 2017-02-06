package whelk.vcopyServlet

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import org.picocontainer.PicoContainer
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.component.Storage
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.BrokenRecordException
import whelk.importer.ImportResult
import whelk.importer.VCopyImporter
import whelk.util.PropertyLoader

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Created by Theodor on 17-01-09
 * Copy of OAIPMH Harvester servlet, but data source is vcopy
 */
@Log
class VCopyImporterServlet extends HttpServlet {

    PicoContainer pico
    int scheduleDelaySeconds = 5
    Properties props = new Properties()
    private Map<String, ScheduledJob> jobs = [:]

    static String SETTINGS_PFX = "harvester:"
    static String DEFAULT_IMPORTER = "whelk.importer.VCopyImporter"
    static int DEFAULT_INTERVAL = 600
    static String DEFAULT_SYSTEM = "vcopy"
    static List<String> DEFAULT_SERVICES = [SETTINGS_PFX + 'auth', SETTINGS_PFX + 'bib', SETTINGS_PFX + 'hold']

    static final ObjectMapper mapper = new ObjectMapper()


    VCopyImporterServlet() {
        log.info("Starting vcopyImporter.")

        props = PropertyLoader.loadProperties('secret','mysql')
        pico = Whelk.getPreparedComponentsContainer(props)
        pico.addComponent(VCopyImporter.class)
        pico.addComponent(new MarcFrameConverter())
        pico.start()
        log.info("Started ...")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        def storage = pico.getComponent(PostgreSQLComponent)
        String html, json
        if (jobs) {
            List<String> services = DEFAULT_SERVICES
            Map state = [:]
            StringBuilder table = new StringBuilder("<table cellspacing=\"10\"><tr><th>&nbsp;</th>")
            table.append("<form method=\"post\">")

            Set catSet = new TreeSet<String>()

            for (service in services) {
                state[service] = storage.loadSettings(service)
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
                System version: ${props.version}<br><br>
                ${table.toString()}
                </form>
                """
            json = mapper.writeValueAsString(state)
        } else {

            html = """
                <html><head><title>OAIPMH Harvest Disabled</title></head>
                <body>

                HARVESTER DISABLED<br/>
                System version ${props.version} is incompatible with data version ${loadDataVersion()}.
                """
            json = mapper.writeValueAsString(["state": "disabled", "system.version": props.version, "data.version": loadDataVersion()])
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
        log.debug("Received post request. Got this: ${request.getParameterMap()}")
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
        if (props.getProperty("version").startsWith(loadDataVersion())) {
            log.info("Initializing vcopy importer. System version: ${pico.getComponent(Whelk.class).version}")
            Storage storage = pico.getComponent(PostgreSQLComponent.class)
            List<String> services = DEFAULT_SERVICES

            ScheduledExecutorService ses = Executors.newScheduledThreadPool(services.size())

            for (service in services) {
                log.info("Setting up schedule for $service")
                String vcopyConnectionString = props.getProperty("mysqlConnectionUrl")
                int scheduleIntervalSeconds = DEFAULT_INTERVAL as int
                String harvesterClass = DEFAULT_IMPORTER
                String sourceSystem = DEFAULT_SYSTEM
                def job = new ScheduledJob(
                        pico.getComponent(Whelk.class) as Whelk,
                        pico.getComponent(Class.forName(harvesterClass)) as VCopyImporter,
                        "${service}",
                        sourceSystem,
                        storage,
                        vcopyConnectionString)
                jobs[service] = job

                try {
                    ses.scheduleWithFixedDelay(job, scheduleDelaySeconds, scheduleIntervalSeconds, TimeUnit.SECONDS)
                } catch (RejectedExecutionException ree) {
                    log.error("execution failed", ree)
                }
            }
            log.info("scheduler started")
        } else {
            log.error("INCOMPATIBLE VERSIONS! Not scheduling any harvesters.")
        }
    }

    String loadDataVersion() {
        def systemSettings = pico.getComponent(PostgreSQLComponent.class).loadSettings("system")
        return systemSettings.get("version")
    }
}

@Log
class ScheduledJob implements Runnable {

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

    String collection, sourceSystem
    Whelk whelk
    VCopyImporter importer
    String vcopyConnectionString
    PostgreSQLComponent storage
    Map whelkState = null
    boolean active = true
    final static long WEEK_MILLIS = 604800000

    ScheduledJob(Whelk whelk, VCopyImporter importer, String coll, String sSystem, PostgreSQLComponent pg, String conStr) {
        this.vcopyConnectionString = conStr
        this.importer = importer
        this.whelk = whelk
        this.collection = coll
        this.storage = pg
        this.sourceSystem = sSystem
        assert storage
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
        storage.saveSettings(collection, whelkState)
    }

    void disable() {
        active = false
        loadWhelkState().put("status", "STOPPED")
        storage.saveSettings(collection, whelkState)
    }

    void enable() {
        active = true
        loadWhelkState().put("status", "IDLE")
        storage.saveSettings(collection, whelkState)
    }

    void setStartDate(Date startDate) {
        loadWhelkState().put("lastImport", startDate.format(DATE_FORMAT))
        storage.saveSettings(collection, whelkState)
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
            whelkState = storage.loadSettings(collection)
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
                    def lastWeeksDate = nextSince[Calendar.DATE] - 7
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

                storage.saveSettings(collection, whelkState)
                ImportResult result = importer.doImport(whelk, collection.replace(VCopyImporterServlet.SETTINGS_PFX,''), sourceSystem, vcopyConnectionString, nextSince)
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
                storage.saveSettings(collection, whelkState)
            }
        }
    }

}

