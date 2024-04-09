package whelk;

import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import whelk.housekeeping.WebInterface;

public class HouseKeepingServer extends XlServer {
    @Override
    protected void configureHandlers(Server server) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        server.setHandler(context);

        ServletHolder holder = new ServletHolder(WebInterface.class);
        holder.setInitOrder(0);
        context.addServlet(holder, "/");
        serveStaticContent(context);
    }

    public static void main(String[] args) throws Exception {
         new HouseKeepingServer().run();
    }
}
