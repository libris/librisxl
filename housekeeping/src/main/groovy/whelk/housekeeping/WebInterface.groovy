package whelk.housekeeping

import whelk.Whelk;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log

import java.time.ZonedDateTime

@CompileStatic
public abstract class HouseKeeper {
    public abstract String getName()
    public abstract String getStatusDescription()
    public abstract void trigger()

    public ZonedDateTime lastFailAt = null
    public ZonedDateTime lastRunAt = null
}

@CompileStatic
@Log
public class WebInterface extends HttpServlet {
    private static final long PERIODIC_TRIGGER_MS = 10 * 1000
    private final Timer timer = new Timer("Housekeeper-timer", true)
    private List<HouseKeeper> houseKeepers = []

    public void init() {
        Whelk whelk = Whelk.createLoadedCoreWhelk()

        houseKeepers = []
        houseKeepers.add(new NotificationSender(whelk))

        for (HouseKeeper hk : houseKeepers) {
            timer.scheduleAtFixedRate({
                try {
                    hk.trigger()
                    hk.lastRunAt = ZonedDateTime.now()
                } catch (Throwable e) {
                    log.error("Could not handle throwable in Housekeeper TimerTask.", e)
                    hk.lastFailAt = ZonedDateTime.now()
                }
            }, PERIODIC_TRIGGER_MS, PERIODIC_TRIGGER_MS)
        }

    }

    public void destroy() {
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
