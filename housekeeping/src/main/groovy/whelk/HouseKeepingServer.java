package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.servlet.DefaultServlet;
import org.eclipse.jetty.ee8.servlet.FilterHolder;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import whelk.datatool.bulkchange.BulkJob;
import whelk.housekeeping.BulkChangePreviewAPI;
import whelk.housekeeping.WebInterface;
import whelk.util.WhelkFactory;
import whelk.util.http.HttpTools;

import javax.servlet.DispatcherType;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.EnumSet;

import static whelk.datatool.bulkchange.BulkJob.BULK_CONTEXT_PATH;
import static whelk.datatool.bulkchange.BulkJob.BULK_REPORTS_PATH;

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
        context.addServlet(BulkChangePreviewAPI.class, BULK_CONTEXT_PATH + "/*");

        serveBulkReports(context);

        serveStaticContent(context);
    }

    private static void serveBulkReports(ServletContextHandler context) {
        String dir;
        try {
            dir = BulkJob.reportBaseDir(WhelkFactory.getSingletonWhelk()).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ServletHolder holder = new ServletHolder("bulk-reports", DefaultServlet.class);
        holder.setInitParameter("resourceBase", dir);
        holder.setInitParameter("dirAllowed", "false");
        holder.setInitParameter("pathInfoOnly", "true");
        var path = BULK_REPORTS_PATH + "/*";
        context.addServlet(holder, path);

        context.addFilter(new FilterHolder(new HttpFilter() {
            @Override
            protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws IOException, ServletException {
                var filePath = req.getPathInfo();
                if (filePath != null && BulkJob.FORBIDDEN_REPORTS.contains(Paths.get(filePath).getFileName().toString())) {
                    HttpTools.sendError(res, 403, "Forbidden");
                } else {
                    super.doFilter(req, res, chain);
                }
            }
        }), path, EnumSet.of(DispatcherType.REQUEST));

        log.info("Serving {} on {}", dir, path);
    }

    public static void main(String[] args) throws Exception {
         new HouseKeepingServer().run();
    }
}
