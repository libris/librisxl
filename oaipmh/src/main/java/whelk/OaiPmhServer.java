package whelk;

import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;
import whelk.export.servlet.OaiPmh;

public class OaiPmhServer extends XlServer {

    @Override
    protected void configureHandlers(Server server) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        server.setHandler(context);

        context.addServlet(MetricsServlet.class, "/metrics");
        context.addServlet(OaiPmh.class, "/");
        serveStaticContent(context);
    }

    public static void main(String[] args) throws Exception {
        new OaiPmhServer().run();
    }
}
