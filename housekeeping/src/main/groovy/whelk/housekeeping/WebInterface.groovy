package whelk.housekeeping

import whelk.Indexing
import whelk.Whelk
import whelk.util.WhelkFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import java.time.ZonedDateTime
import it.sauronsoftware.cron4j.Scheduler

@CompileStatic
@Log
public abstract class HouseKeeper {
    public abstract String getName()
    public abstract String getStatusDescription()
    public abstract String getCronSchedule()
    public abstract void trigger()

    public ZonedDateTime lastFailAt = null
    public ZonedDateTime lastRunAt = null

    synchronized void _trigger() {
        try {
            trigger()
            lastRunAt = ZonedDateTime.now()
        } catch (Throwable e) {
            log.error("Could not handle throwable in Housekeeper TimerTask.", e)
            lastFailAt = ZonedDateTime.now()
        }
    }
}

@CompileStatic
@Log
public class WebInterface extends HttpServlet {
    private Map<String, HouseKeeper> houseKeepersById = [:]
    Scheduler cronScheduler = new Scheduler()

    public void init() {
        Whelk whelk = WhelkFactory.getSingletonWhelk()

        Indexing.start(whelk)

        List<HouseKeeper> houseKeepers = [
                // Automatic generation is disabled for now, may need design changes approved before activation.
                //new NotificationGenerator(whelk),
                //new NotificationSender(whelk),

                new InquirySender(whelk),
                new NotificationCleaner(whelk),
                new ImageLinker(whelk),
                new ExportSizePredictor(whelk),
                new ScriptRunner(whelk, "wikidatalinking.groovy", "0 19 22 2,4,6,8,10,12 *"),
                new ScriptRunner(whelk, "lxl-3599-instance-types-from-mediaterm.groovy", "0 20 1 * *"),
                new ScriptRunner(whelk, "lxl-3601-change-type-Instance-to-Print.groovy", "0 20 2 * *"),
                new ScriptRunner(whelk, "lxl-3785-supplementTo-isIssueOf.groovy", "0 20 3 * *"),
                new ScriptRunner(whelk, "lxl-3785-fix-title-chars.groovy", "0 20 4 * *"),
                new ScriptRunner(whelk, "lxl-3873-remove-classification-without-code.groovy", "0 20 5 * *"),
                new BulkChangeRunner(whelk),
                new HistoryArchiver(whelk),
        ]

        houseKeepers.each { hk ->
            String id = cronScheduler.schedule(hk.getCronSchedule(), {
                hk._trigger()
            })
            houseKeepersById.put(id, hk)
        }
        cronScheduler.start()
    }

    public void destroy() {
        cronScheduler.stop()
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) {
        StringBuilder sb = new StringBuilder()
        sb.append("Active housekeepers: " + houseKeepersById.size() + "\n")
        sb.append("--------------\n")
        for (String key : houseKeepersById.keySet()) {
            HouseKeeper hk = houseKeepersById[key]
            sb.append(hk.getName() + "\n")
            if (hk.lastRunAt)
                sb.append("Last run at: " + hk.lastRunAt + "\n")
            else
                sb.append("Has never run\n")
            if (hk.lastFailAt)
                sb.append("Last failed at: " + hk.lastFailAt + "\n")
            else
                sb.append("No failures\n")
            sb.append("Status:\n")
            sb.append(hk.statusDescription+"\n")
            sb.append("Execution schedule:\n")
            sb.append(hk.cronSchedule+"\n")
            sb.append("To force immediate execution, POST to:\n" + req.getRequestURL() + key + "\n")
            sb.append(req.getRequestURL() + hk.class.getSimpleName() + "\n")
            sb.append("--------------\n")
        }
        res.setStatus(HttpServletResponse.SC_OK)
        res.setContentType("text/plain")
        res.getOutputStream().print(sb.toString())
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) {
        String key = req.getRequestURI().split("/").last()
        if (houseKeepersById[key]) {
            houseKeepersById[key]._trigger()
        } else {
            houseKeepersById.values()
                    .findAll{ it.class.getSimpleName() == key }
                    .each {it._trigger() }
        }
    }
}
