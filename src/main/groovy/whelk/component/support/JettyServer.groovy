package whelk.component.support

import java.security.ProtectionDomain

import org.eclipse.jetty.*
import org.eclipse.jetty.server.*
import org.eclipse.jetty.server.nio.*
import org.eclipse.jetty.servlet.*
import org.eclipse.jetty.util.log.Log
import org.eclipse.jetty.util.log.Slf4jLog
import org.eclipse.jetty.webapp.*


class JettyServer {

    static void main(String[] args) {
        int port = 8180
        String contextPath = "/"

        def cli = new CliBuilder(usage: 'JettyServer [-h] [-p port] [-c context-path]')
        cli.with {
            h longOpt: 'help', 'Show usage information'
            p longOpt: 'port', args: 1, 'Use port'
            c longOpt: 'context-path', args: 1, 'Use contextPath'
        }
        def options = cli.parse(args)
        if (options.h) {
            cli.usage()
            return
        }
        if (options.p) {
            port = options.p
        }
        if (options.c) {
            contextPath = options.c
        }


        Server server = new Server();

        //Connector connector = new ServerConnector(server, new HttpConnectionFactory()) // Jetty 9
        Connector connector = new SelectChannelConnector()
        connector.setPort(port)
        server.addConnector(connector)
        server.setStopAtShutdown(true)
        WebAppContext context = new WebAppContext()

        context.setServer(server)
        context.setContextPath(contextPath)

        ProtectionDomain protectionDomain = JettyServer.class.getProtectionDomain()
        URL location = protectionDomain.getCodeSource().getLocation()
        context.setWar(location.toExternalForm())
        println "WAR URL: ${location.toExternalForm()}"
        server.setHandler(context)

        try {
            server.start();
            System.in.read();
            server.stop();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(100);
        }

    }
}
