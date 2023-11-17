package whelk.housekeeping

import whelk.Whelk;
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
        Whelk whelk = Whelk.createLoadedSearchWhelk()

        List<HouseKeeper> houseKeepers = [
                new NotificationGenerator(whelk),
                new NotificationSender(whelk),
                new InquirySender(whelk),
                new NotificationCleaner(whelk),
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
            sb.append("--------------\n")
        }
        res.setStatus(HttpServletResponse.SC_OK)
        res.setContentType("text/plain")
        res.getOutputStream().print(sb.toString())
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) {
        String key = req.getRequestURI().split("/").last()
        houseKeepersById[key]._trigger()
    }
}
