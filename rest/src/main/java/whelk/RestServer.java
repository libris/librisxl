package whelk;

import com.thetransactioncompany.cors.CORSFilter;
import io.prometheus.client.exporter.MetricsServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.servlet.FilterHolder;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

public class RestServer extends XlServer {
    private final static Logger log = LogManager.getLogger(XlServer.class);

    private static final String REMOTE_SEARCH_PATH = "/_remotesearch";
    private static final String USERDATA_PATH = "/_userdata/*";

    @Override
    protected void configureHandlers(Server server) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        // TODO: AMBIGUOUS_EMPTY_SEGMENT care of double slashes in id.kb.se paths from nginx
        // try to eliminate them there instead?
        // AMBIGUOUS_PATH_SEPARATOR allows e.g. /term/gmgpc%2F%2Fswe/Kartor
        // https://jetty.org/docs/jetty/12/programming-guide/server/compliance.html
        // https://javadoc.jetty.org/jetty-12/org/eclipse/jetty/http/UriCompliance.Violation.html
        Arrays.stream(server.getConnectors()).forEach(
                connector -> { connector
                        .getConnectionFactory(HttpConnectionFactory.class)
                        .getHttpConfiguration()
                        .setUriCompliance(UriCompliance.DEFAULT
                                .with("DOUBLE_SLASH",
                                        UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT,
                                        UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR
                                )
                        );
                }
        );
        var rewriteHandler = new RewriteHandler();
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

        serveStaticContent(context);

        context.addEventListener(new MarcFrameConverterInitializer());
    }

    public static void main(String[] args) throws Exception {
        new RestServer().run();
    }
}
