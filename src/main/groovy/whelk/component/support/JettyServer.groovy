package whelk.component.support

import groovy.util.logging.Slf4j as Log

import org.eclipse.jetty.*
import org.eclipse.jetty.server.*
import org.eclipse.jetty.servlet.*

import whelk.*

@Log
class JettyServer {

    static void main(String[] args) {
        int port = 8080
        String contextPath = "/"

        def cli = new CliBuilder(usage: 'JettyServer -[chflms] [date] [prefix]')
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

        Server server = new Server(port)

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS)
        context.setContextPath(contextPath)
        server.setHandler(context)

        context.addServlet(new ServletHolder(new StandardWhelk()),"/*")

        server.start();
        server.join();
    }
}
