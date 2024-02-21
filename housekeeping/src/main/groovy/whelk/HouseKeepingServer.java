package whelk;

import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;
import whelk.housekeeping.WebInterface;

public class HouseKeepingServer extends XlServer {
    @Override
    protected void configureHandlers(Server server) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        server.setHandler(context);

        context.addServlet(WebInterface.class, "/");
        serveStaticContent(context);
    }

    public static void main(String[] args) throws Exception {
         new HouseKeepingServer().run();
    }
}
