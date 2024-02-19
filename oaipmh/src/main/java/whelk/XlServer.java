package whelk;

import io.prometheus.client.exporter.MetricsServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;
import whelk.export.servlet.OaiPmh;
import whelk.meta.WhelkConstants;

public class XlServer {
    private final static Logger log = LogManager.getLogger(XlServer.class);

    public void run() throws Exception {
        int port = WhelkConstants.getHttpPort();

        var server = new Server(port);

        configure(server);

        server.start();
        log.info("Started server on port {}", port);
        server.join();
    }

    protected void configure(Server server ) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        server.setHandler(context);

        context.addServlet(MetricsServlet.class, "/metrics");
        context.addServlet(OaiPmh.class, "/");
    }

    public static void main(String[] args) throws Exception {
        var xlServer = new XlServer();
        xlServer.run();
    }
}
