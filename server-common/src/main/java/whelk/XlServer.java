package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.servlet.DefaultServlet;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.AsyncRequestLogWriter;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class XlServer {
    private final static Logger log = LogManager.getLogger(XlServer.class);

    public void run() throws Exception {
        var server = createServer();
        configure(server);
        server.start();
        server.join();
    }

    protected abstract void configureHandlers(Server server);

    protected void configure(Server server) throws IOException {
        configureHandlers(server);
        setupAccessLogging(server);
    }

    // TODO: review this...
    protected Server createServer() {
        int maxConnections = Configuration.getMaxConnections();
        var queue = new ArrayBlockingQueue<Runnable>(1);
        var pool = new ExecutorThreadPool(maxConnections, Math.min(10, maxConnections), queue);

        var server = new Server(pool);

        int port = Configuration.getHttpPort();

        var httpConfig = new HttpConfiguration();
        httpConfig.setIdleTimeout(20000);
        try (var http = new ServerConnector(server, new HttpConnectionFactory(httpConfig))) {
            http.setPort(port);
            http.setAcceptQueueSize(0);
            server.setConnectors(new Connector[]{ http });
            log.info("Started server on port {}", port);
        }

        server.addBean(new ConnectionLimit(maxConnections, server));

        return server;
    }

    protected void setupAccessLogging(Server server) throws IOException {
        Path logRoot = Configuration.getLogRoot();
        if (!Files.isDirectory(logRoot)) {
            Files.createDirectories(logRoot);
        }
        var requestLogWriter = new AsyncRequestLogWriter();
        requestLogWriter.setAppend(true);
        requestLogWriter.setFilename(logRoot.resolve("access_log.yyyy_MM_dd.txt").toString());
        requestLogWriter.setTimeZone(TimeZone.getDefault().getID());
        requestLogWriter.setRetainDays(14);
        var requestLog = new CustomRequestLog(requestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT);
        server.setRequestLog(requestLog);
    }

    protected void serveStaticContent(ServletContextHandler context) {
        var dir = Configuration.getStaticContentDir();
        ServletHolder staticContent = new ServletHolder("static", DefaultServlet.class);
        staticContent.setInitParameter("resourceBase", Configuration.getStaticContentDir());
        staticContent.setInitParameter("dirAllowed", "true");
        staticContent.setInitParameter("pathInfoOnly", "true");
        var staticPath = "/static/*";
        context.addServlet(staticContent, staticPath);
        log.info("Serving {} on {}", dir, staticPath);
    }
}
