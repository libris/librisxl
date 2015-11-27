package whelk.servlet

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.PicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.Document
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.OaiPmhImporter
import whelk.scheduler.ScheduledOperator

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Created by markus on 15-09-03.
 */
@Log
class OaiPmhImporterServlet extends HttpServlet {

    PicoContainer pico
    int scheduleDelaySeconds = 10
    int scheduleIntervalSeconds = 30
    Properties props = new Properties()
    private Map<String,ScheduledJob> jobs = [:]

    static final ObjectMapper mapper = new ObjectMapper()


    public OaiPmhImporterServlet() {
        log.info("Starting oaipmhimporter.")

        Properties mainProps = new Properties()

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                ? new FileInputStream(System.getProperty("xl.secret.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("secret.properties") )

        InputStream oaipmhConfig = ( System.getProperty("xl.oaipmh.properties")
                ? new FileInputStream(System.getProperty("xl.oaipmh.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("oaipmh.properties") )


        props.load(secretsConfig)
        props.load(oaipmhConfig)

        pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))

        //pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(ElasticSearch.class)
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)
        pico.as(Characteristics.USE_NAMES).addComponent(OaiPmhImporter.class)
        pico.addComponent(new MarcFrameConverter())
        pico.addComponent(Whelk.class)

        pico.start()

        log.info("Started ...")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        def storage = pico.getComponent(PostgreSQLComponent)

        List datasets = props.scheduledDatasets.split(",")
        def state = [:]
        StringBuilder table = new StringBuilder("<table cellspacing=\"10\"><tr><th>&nbsp;</th>")
        table.append("<form method=\"post\">")

        Set catSet = new TreeSet<String>()

        for (dataset in datasets) {
            state[dataset] = storage.loadSettings(dataset)
            catSet.addAll(state[dataset].keySet())
            table.append("<th>$dataset</th>")
        }
        table.append("</tr>")
        List categories = catSet.toList()

        log.info("state: $state")
        log.info("Categories: $catSet")

        int i = 0
        for (cat in categories) {
            table.append("<tr><td align=\"right\"><b>$cat</b></td>")
            for (dataset in datasets) {
                table.append("<td>${state.get(dataset).get(cat) != null ? state.get(dataset).get(cat) : "&nbsp;"}</td>")
            }
            table.append("</tr>")
        }
        table.append("<tr><td><input type=\"submit\" name=\"action_all\" value=\"stop all\"></td>")
        for (dataset in datasets) {
            table.append("<td><input type=\"submit\" name=\"action_${dataset}\" value=\"${jobs[dataset].active ? "stop" : "start"}\"></td>")
        }
        table.append("</tr>")

        table.append("</form></table>")

        String html =
                """
                <html><head><title>OAIPMH Harvester control panel</title></head>
                <body>
                ${table.toString()}
                </form>
                """


        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.print(html);
        out.flush();
    }

    void doPost(HttpServletRequest request, HttpServletResponse response) {
        log.info("Received post request. Got this: ${request.getParameterMap()}")
        for (reqs in request.getParameterNames()) {
            if (reqs == "action_all") {
                for (job in jobs) {
                    job.value.disable()
                }
            } else {
                jobs.get(reqs.substring(7)).toggleActive()
            }
        }
        response.sendRedirect(request.getRequestURL().toString())
    }

    void init() {

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3)
        List datasets = props.scheduledDatasets.split(",")
        for (dataset in datasets) {
            log.info("Setting up schedule for $dataset")
            def job = new ScheduledJob(pico.getComponent(OaiPmhImporter.class), dataset, pico.getComponent(PostgreSQLComponent.class))
            jobs[dataset] = job
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

    String dataset
    OaiPmhImporter importer
    PostgreSQLComponent storage
    Map whelkState = null
    boolean active = true

    ScheduledJob(OaiPmhImporter imp, String ds, PostgreSQLComponent pg) {
        this.importer = imp
        this.dataset = ds
        this.storage = pg
        assert storage
        assert dataset
    }

    void toggleActive() {
        active = !active
        loadWhelkState().put("status", (active ? "IDLE" : "STOPPED"))
        storage.saveSettings(dataset, whelkState)
    }

    void disable() {
        active = false
        println("Setting ws: stopped")
        loadWhelkState().put("status", "STOPPED")
        storage.saveSettings(dataset, whelkState)
    }

    void enable() {
        active = true
        loadWhelkState().put("status", "IDLE")
        storage.saveSettings(dataset, whelkState)
    }

    Map loadWhelkState() {
        if (!whelkState) {
            log.info("Loading current state from storage ...")
            whelkState = storage.loadSettings(dataset)
            log.info("Loaded state for $dataset : $whelkState")
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
                    log.info("Importer has no state for last import from $dataset. Setting last week (${nextSince})")
                }
                //nextSince = new Date(0) //sneeking past next date
                if (nextSince.after(new Date())) {
                    log.warn("Since is slipping ... Is now ${nextSince}. Resetting to now()")
                    nextSince = new Date()
                }
                log.debug("Executing OAIPMH import for $dataset since $nextSince from ${importer.serviceUrl}")
                whelkState.put("status", "RUNNING")

                storage.saveSettings(dataset, whelkState)
                def result = importer.doImport(dataset, null, -1, true, true, nextSince)
                log.trace("Import completed, result: $result")

                if (result.numberOfDocuments > 0 || result.numberOfDeleted > 0 || result.numberOfDocumentsSkipped > 0) {
                    log.info("Imported ${result.numberOfDocuments} documents and deleted ${result.numberOfDeleted} for $dataset. Last record has datestamp: ${result.lastRecordDatestamp.format(DATE_FORMAT)}")
                    whelkState.put("lastImportNrImported", result.numberOfDocuments)
                    whelkState.put("lastImportNrDeleted", result.numberOfDeleted)
                    whelkState.put("lastImportNrSkipped", result.numberOfDocumentsSkipped)
                    whelkState.put("lastImport", result.lastRecordDatestamp.format(DATE_FORMAT))

                } else {
                    log.debug("Imported ${result.numberOfDocuments} document for $dataset.")
                    whelkState.put("lastImport", currentSince.format(DATE_FORMAT))
                }
                whelkState.put("status", "IDLE")
                whelkState.put("lastRunNrImported", result.numberOfDocuments)
                whelkState.put("lastRun", new Date().format(DATE_FORMAT))
            } catch (Exception e) {
                log.error("Something failed: ${e.message}", e)
            } finally {
                log.debug("Saving state $whelkState")
                storage.saveSettings(dataset, whelkState)
            }
        }
    }

}

