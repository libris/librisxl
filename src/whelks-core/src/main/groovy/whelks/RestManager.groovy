package se.kb.libris.conch

import java.util.regex.Pattern 
import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.representation.*
import org.restlet.routing.*

import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.*
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.plugin.external.*

@Log
class RestManager extends Application {

    final String WHELKCONFIGFILE = "file:src/main/resources/barebones-whelks.json"

    def whelks = []

    RestManager(Context parentContext) {
        super(parentContext)
        log.debug("Using file encoding: " + System.getProperty("file.encoding"));
        init()
    }

    void init() {
        URI whelkconfig = new URI(System.getProperty("whelk.config.uri", WHELKCONFIGFILE))
        log.info("Initializing whelks using definitions in $whelkconfig")
        def wi = new WhelkInitializer(whelkconfig.toURL().newInputStream())
        whelks = wi.getWhelks()
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx)


        router.attach("/", new Restlet() {
            void handle(Request request, Response response) {
                if (request.method == Method.POST) {
                    // TODO: Handle uploaded unnamed document
                }
                response.setEntity("Try a URI for a document, or ${request.rootRef}/_find?q=query to search", MediaType.TEXT_PLAIN)
            }
        })

        log.debug("Looking for suitable APIs to attach")

        whelks.each {
            log.debug("Getting APIs for whelk ${it.prefix}")
            for (api in it.getAPIs()) {
                if (!api.varPath) {
                    router.attach(api.path, api)
                }
            }
            for (api in it.getAPIs()) {
                log.debug("Attaching ${api.class.name} at ${api.path}")
                if (api.varPath) {
                    router.attach(api.path, api).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))
                }             
            }
        }

        /*
        def dr = new DocumentRestlet()
        whelks['suggest'].addPlugin(dr)
        router.attach("{path}", dr).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))

        */
        return router
    }
}
