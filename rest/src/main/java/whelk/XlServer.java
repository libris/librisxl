package whelk;

import com.thetransactioncompany.cors.CORSFilter;
import io.prometheus.client.exporter.MetricsServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.servlet.DefaultServlet;
import org.eclipse.jetty.ee8.servlet.FilterHolder;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.rewrite.handler.CompactPathRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.AsyncRequestLogWriter;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import whelk.meta.WhelkConstants;
import whelk.rest.api.ConverterAPI;
import whelk.rest.api.Crud;
import whelk.rest.api.DuplicatesAPI;
import whelk.rest.api.HoldAPI;
import whelk.rest.api.LegacyMarcAPI;
import whelk.rest.api.MarcFrameConverterInitializer;
import whelk.rest.api.MarcframeData;
import whelk.rest.api.RecordRelationAPI;
import whelk.rest.api.RefreshAPI;
import whelk.rest.api.RemoteSearchAPI;
import whelk.rest.api.TransliterationAPI;
import whelk.rest.api.UserDataAPI;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

import static whelk.meta.WhelkConstants.getLogRoot;

public class XlServer {
    private final static Logger log = LogManager.getLogger(XlServer.class);

    private static final String REMOTE_SEARCH_PATH = "/_remotesearch";
    private static final String USERDATA_PATH = "/_userdata/*";

    public void run() throws Exception {
        int port = WhelkConstants.getHttpPort();

        var server = new Server(port);

        configure(server);
        setupAccessLogging(server);

        server.start();
        log.info("Started server on port {}", port);
        server.join();
    }

    protected void configure(Server server) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        // TODO: this takes care of double slashes in id.kb.se paths from nginx
        // try to eliminate them there instead?
        Arrays.stream(server.getConnectors()).forEach(
                connector -> { connector
                        .getConnectionFactory(HttpConnectionFactory.class)
                        .getHttpConfiguration()
                        .setUriCompliance(UriCompliance.DEFAULT.with("DOUBLE_SLASH", UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT));
                }
        );
        var rewriteHandler = new RewriteHandler();
        rewriteHandler.addRule(new CompactPathRule());
        rewriteHandler.setHandler(context);

        server.setHandler(rewriteHandler);

        context.addFilter(CORSFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST))
                .setInitParameters(Map.of(
                        "cors.exposedHeaders", "ETag,Location",
                        "cors.supportedMethods", "GET, POST, PUT, DELETE, HEAD, OPTIONS"
                ));

        // TODO: configure mockAuthentication
        context.addFilter(AuthenticationFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST))
                .setInitParameters(Map.of(
                        "supportedMethods", "POST, PUT, DELETE",
                        "whitelistedPostEndpoints", "/_convert,/_transliterate",
                        "mockAuthentication", "false"
                ));

        var authenticationGet = new FilterHolder(AuthenticationFilter.class);
        authenticationGet.setName("AuthenticationFilterGet");
        authenticationGet.setInitParameters(Map.of(
                "supportedMethods", "GET",
                "mockAuthentication", "false"
        ));
        context.addFilter(authenticationGet, USERDATA_PATH, EnumSet.of(DispatcherType.REQUEST));
        context.addFilter(authenticationGet, REMOTE_SEARCH_PATH, EnumSet.of(DispatcherType.REQUEST));

        context.addServlet(Crud.class, "/*");
        context.addServlet(MarcframeData.class, "/sys/marcframe.json");

        // TODO load-on-startup ?
        context.addServlet(MetricsServlet.class, "/metrics");

        context.addServlet(RemoteSearchAPI.class, REMOTE_SEARCH_PATH);
        context.addServlet(UserDataAPI.class, USERDATA_PATH);
        context.addServlet(TransliterationAPI.class, "/_transliterate/*");

        context.addServlet(ConverterAPI.class, "/_convert");
        context.addServlet(LegacyMarcAPI.class, "/_compilemarc");

        context.addServlet(RefreshAPI.class, "/_refresh");
        context.addServlet(HoldAPI.class, "/_findhold");
        context.addServlet(RecordRelationAPI.class, "/_dependencies");
        context.addServlet(DuplicatesAPI.class, "/_duplicates");

        context.addServlet(se.kb.libris.digi.DigitalReproductionAPI.class, "/_reproduction");

        ServletHolder staticContent = new ServletHolder("static", DefaultServlet.class);
        staticContent.setInitParameter("resourceBase", WhelkConstants.getStaticContentDir());
        staticContent.setInitParameter("dirAllowed", "true");
        staticContent.setInitParameter("pathInfoOnly", "true");
        context.addServlet(staticContent, "/static/*");

        context.addEventListener(new MarcFrameConverterInitializer());
    }

    private void setupAccessLogging(Server server) throws IOException {
        Path logRoot = getLogRoot();
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

    public static void main(String[] args) throws Exception {
        var xlServer = new XlServer();
        xlServer.run();
    }
}
