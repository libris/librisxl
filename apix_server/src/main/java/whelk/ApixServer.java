package whelk;

import io.prometheus.client.exporter.MetricsServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee8.nested.ServletConstraint;
import org.eclipse.jetty.ee8.security.ConstraintAware;
import org.eclipse.jetty.ee8.security.ConstraintMapping;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.resource.PathResourceFactory;
import org.eclipse.jetty.util.resource.Resource;
import whelk.apixserver.ApixCatServlet;
import whelk.apixserver.ApixSearchServlet;

public class ApixServer extends XlServer {
    private static final Logger log = LogManager.getLogger(ApixServer.class);

    private static final String USER_PROPERTIES_PATH_PARAMETER = "xl.apix-users.properties";

    @Override
    protected void configureHandlers(Server server) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SECURITY);
        context.setContextPath("/");

        if (context.getSecurityHandler() instanceof ConstraintAware security)
        {
            ServletConstraint constraint = new ServletConstraint();
            constraint.setAuthenticate(true);
            constraint.setRoles(new String[]{ "apix" });

            ConstraintMapping mapping = new ConstraintMapping();
            mapping.setPathSpec("/apix/*");
            mapping.setConstraint(constraint);
            security.addConstraintMapping(mapping);

            // One row per user:
            // <username>: <password> [, <role>...}
            // for example:
            // foo: bar, apix
            String fileName = System.getProperty(USER_PROPERTIES_PATH_PARAMETER, "");
            if ("".equals(fileName)) {
                throw new IllegalStateException(USER_PROPERTIES_PATH_PARAMETER + " not configured");
            }

            Resource config = new PathResourceFactory().newResource(fileName);
            LoginService loginService = new HashLoginService("ApixRealm", config);
            server.addBean(loginService);
            context.getSecurityHandler().setAuthMethod("BASIC");
            context.getSecurityHandler().setLoginService(loginService);
            log.info("Getting APIX users from {}", fileName);
        }
        else
        {
            throw new RuntimeException("Not a ConstraintAware SecurityHandler");
        }

        context.addServlet(MetricsServlet.class, "/metrics");
        context.addServlet(ApixCatServlet.class, "/apix/0.1/cat/*");
        context.addServlet(ApixSearchServlet.class, "/apix/0.1/cat/libris/search");

        server.setHandler(context);
    }

    public static void main(String[] args) throws Exception {
        var xlServer = new XlServer();
        xlServer.run();
    }
}
