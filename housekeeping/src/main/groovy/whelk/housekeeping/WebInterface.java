package whelk.housekeeping;

import it.sauronsoftware.cron4j.Scheduler;
import whelk.Whelk;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WebInterface extends WhelkHttpServlet {
    private final Map<String, HouseKeeper> houseKeepersById = new LinkedHashMap<>();
    private final Scheduler cronScheduler = new Scheduler();

    @Override
    public void init(Whelk whelk) {
        List<HouseKeeper> houseKeepers = List.of(
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
                new HistoryArchiver(whelk)
        );

        for (HouseKeeper hk : houseKeepers) {
            String id = cronScheduler.schedule(hk.getCronSchedule(), hk::_trigger);
            houseKeepersById.put(id, hk);
        }
        cronScheduler.start();
    }

    @Override
    public void destroy() {
        cronScheduler.stop();
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("Active housekeepers: ").append(houseKeepersById.size()).append("\n");
        sb.append("--------------\n");
        for (String key : houseKeepersById.keySet()) {
            HouseKeeper hk = houseKeepersById.get(key);
            sb.append(hk.getName()).append("\n");
            if (hk.lastRunAt != null)
                sb.append("Last run at: ").append(hk.lastRunAt).append("\n");
            else
                sb.append("Has never run\n");
            if (hk.lastFailAt != null)
                sb.append("Last failed at: ").append(hk.lastFailAt).append("\n");
            else
                sb.append("No failures\n");
            sb.append("Status:\n");
            sb.append(hk.getStatusDescription()).append("\n");
            sb.append("Execution schedule:\n");
            sb.append(hk.getCronSchedule()).append("\n");
            sb.append("To force immediate execution, POST to:\n").append(req.getRequestURL()).append(key).append("\n");
            sb.append(req.getRequestURL()).append(hk.getClass().getSimpleName()).append("\n");
            sb.append("--------------\n");
        }
        res.setStatus(HttpServletResponse.SC_OK);
        res.setContentType("text/plain");
        res.getOutputStream().print(sb.toString());
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse res) {
        String[] pathSegments = req.getRequestURI().split("/");
        String key = pathSegments[pathSegments.length - 1];

        HouseKeeper houseKeeper = houseKeepersById.get(key);
        if (houseKeeper != null) {
            houseKeeper._trigger();
        } else {
            for (HouseKeeper hk : houseKeepersById.values()) {
                if (hk.getClass().getSimpleName().equals(key)) {
                    hk._trigger();
                }
            }
        }
    }
}
