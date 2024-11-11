package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.servlet.DefaultServlet;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import whelk.datatool.bulkchange.BulkJob;
import whelk.housekeeping.BulkChangePreviewAPI;
import whelk.housekeeping.WebInterface;
import whelk.util.WhelkFactory;

import java.io.IOException;

public class HouseKeepingServer extends XlServer {
    private final static Logger log = LogManager.getLogger(HouseKeepingServer.class);

    @Override
    protected void configureHandlers(Server server) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        server.setHandler(context);

        ServletHolder holder = new ServletHolder(WebInterface.class);
        holder.setInitOrder(0);
        context.addServlet(holder, "/");
        context.addServlet(BulkChangePreviewAPI.class, "/_bulk-change/*");

        serveBulkReports(context);

        serveStaticContent(context);
    }

    // TODO Access control!!
    private static void serveBulkReports(ServletContextHandler context) {
        String dir = null;
        try {
            dir = BulkJob.reportBaseDir(WhelkFactory.getSingletonWhelk()).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //var dir = "/home/OloYli/kod/librisxl/housekeeping/logs/bulk-change-reports";
        ServletHolder staticContent = new ServletHolder("bulk-reports", DefaultServlet.class);
        staticContent.setInitParameter("resourceBase", dir);
        staticContent.setInitParameter("dirAllowed", "true");
        staticContent.setInitParameter("pathInfoOnly", "true");
        var path = "/_bulk-change/reports/*";
        context.addServlet(staticContent, path);
        log.info("Serving {} on {}", dir, path);
    }

    public static void main(String[] args) throws Exception {
         new HouseKeepingServer().run();
    }
}
