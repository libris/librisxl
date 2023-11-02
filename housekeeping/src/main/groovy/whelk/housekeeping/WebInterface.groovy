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
    private List<HouseKeeper> houseKeepers = []
    Scheduler cronScheduler = new Scheduler()

    public void init() {
        Whelk whelk = Whelk.createLoadedCoreWhelk()

        houseKeepers = []
        houseKeepers.add(new NotificationGenerator(whelk))
        houseKeepers.add(new NotificationSender(whelk))

        houseKeepers.each { hk ->
            cronScheduler.schedule(hk.getCronSchedule(), {
                hk._trigger()
            })
        }
        cronScheduler.start()
    }

    public void destroy() {
        cronScheduler.stop()
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) {
        StringBuilder sb = new StringBuilder()
        sb.append("Active housekeepers: " + houseKeepers.size() + "\n")
        sb.append("--------------\n")
        for (HouseKeeper hk : houseKeepers) {
            sb.append(hk.getName() + "\n")
            if (hk.lastRunAt)
                sb.append("last run at: " + hk.lastRunAt + "\n")
            else
                sb.append("has never run\n")
            if (hk.lastFailAt)
                sb.append("last failed at: " + hk.lastFailAt + "\n")
            else
                sb.append("no failures\n")
            sb.append("status:\n")
            sb.append(hk.statusDescription+"\n")
            sb.append("--------------\n")
        }
        res.setStatus(HttpServletResponse.SC_OK)
        res.setContentType("text/plain")
        res.getOutputStream().print(sb.toString())
    }
}
