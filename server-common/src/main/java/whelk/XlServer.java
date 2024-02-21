package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.server.AsyncRequestLogWriter;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class XlServer {
    private final static Logger log = LogManager.getLogger(XlServer.class);

    public void run() throws Exception {
        int port = Configuration.getHttpPort();

        var server = new Server(port);

        configure(server);

        server.start();
        log.info("Started server on port {}", port);
        server.join();
    }

    protected void configure(Server server) throws IOException {
        configureHandlers(server);
        setupAccessLogging(server);
    }

    protected abstract void configureHandlers(Server server);

    private void setupAccessLogging(Server server) throws IOException {
        Path logRoot = Configuration.getLogRoot();
        if (!Files.isDirectory(logRoot)) {
            Files.createDirectories(logRoot);
        }
        AsyncRequestLogWriter requestLogWriter = new AsyncRequestLogWriter();
        requestLogWriter.setAppend(true);
        requestLogWriter.setFilename(logRoot.resolve("access.log").toString());
        requestLogWriter.setRetainDays(3);
        RequestLog requestLog = new CustomRequestLog(requestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT);
        server.setRequestLog(requestLog);
    }
}
