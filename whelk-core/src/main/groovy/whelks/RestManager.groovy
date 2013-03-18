package se.kb.libris.whelks.api

import java.util.regex.Pattern 
import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.representation.*
import org.restlet.routing.*

import org.codehaus.jackson.map.*

import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.*
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.plugin.external.*

@Log
class RestManager extends Application {

    def whelks = []

    RestManager(Context parentContext) {
        super(parentContext)
        log.debug("Using file encoding: " + System.getProperty("file.encoding"));
        log.debug("Retrievieng whelks from JNDI ...")
        whelks = new javax.naming.InitialContext().lookup("whelks")
        log.debug("Found these whelks: $whelks")
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx)


        /*router.attach("/", new Restlet() {
            ObjectMapper mapper = new ObjectMapper()

            void handle(Request request, Response response) {
                if (request.method == Method.POST) {
                    // TODO: Handle uploaded unnamed document
                }
                if (request.method == Method.GET) {
                    def discoveryAPI = new DiscoveryAPI(this.whelk)
                    def uri = new URI(discoveryAPI.path)
                    log.info "Handling inbound route to " + uri
                    reponse.serverRedirect(discoveryAPI, new Reference(uri), request, response)
            } else if (request.method == Method.PUT || request.method == Method.POST) {
                /*def documentAPI = new DocumentRestlet(this.whelk)
                response.serverRedirect(documentAPI, new Reference(new URI(documentAPI.path)), request, response)
            }
                def wlist = ["available_whelks":whelks.collect { it.prefix }]
                response.setEntity(mapper.writeValueAsString(wlist), MediaType.APPLICATION_JSON)
            }
        })*/

        log.debug("Looking for suitable APIs to attach")

        whelks.each {
            log.debug("Attaching RootRoute API")
            def rapi = new RootRouteRestlet(it)
            router.attach(rapi.path, rapi)
            log.debug("Attaching Discovery API")
            def dapi = new DiscoveryAPI(it)
            router.attach(dapi.path, dapi)
            log.debug("Getting APIs for whelk ${it.prefix}")
            for (api in it.getAPIs()) {
                if (!api.varPath) {
                    router.attach(api.path, api)
                }
            }
            for (api in it.getAPIs()) {
                log.debug("Attaching ${api.class.name} at ${api.path}")
                if (api.varPath) {
                    router.attach(api.path, api).template.variables.put("identifier", new Variable(Variable.TYPE_URI_PATH))
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
