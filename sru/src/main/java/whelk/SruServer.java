package whelk;

import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import whelk.sru.servlet.SruServlet;
import whelk.sru.servlet.XSearchServlet;

public class SruServer extends XlServer {

    @Override
    protected void configureHandlers(Server server) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");


        server.setHandler(context);

        context.addServlet(MetricsServlet.class, "/metrics");
        context.addServlet(SruServlet.class, "/");

        ServletHolder holder = new ServletHolder(XSearchServlet.class);
        holder.setInitOrder(1); // init on server startup
        context.addServlet(holder, "/xsearch");

        serveStaticContent(context);
    }

    public static void main(String[] args) throws Exception {
        new SruServer().run();
    }
}
