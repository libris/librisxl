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
import se.kb.libris.whelks.http.*
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

        log.debug("Looking for suitable APIs to attach")

        def rootwhelk

        whelks.each {
            if (it instanceof HttpWhelk && it.contentRoot == "") {
                rootwhelk = it
            } else {
                attachApis(router, it)
            }
        }
        log.debug("Attached subwhelks.")
        if (rootwhelk) {
            log.debug("Now attaching root whelk.")
            attachApis(router, rootwhelk)
        }

        return router
    }

    void attachApis(router, whelk) {
        log.debug("Attaching RootRoute API")
        def rapi = new RootRouteRestlet(whelk)
        router.attach(rapi.path, rapi)
        log.debug("Attaching Discovery API")
        def dapi = new DiscoveryAPI(whelk)
        router.attach(dapi.path, dapi)
        log.debug("Getting APIs for whelk ${whelk.id}")
        for (api in whelk.getAPIs()) {
            if (!api.varPath) {
                log.debug("API is $api")
                router.attach(api.path, api)
            }
        }
        for (api in whelk.getAPIs()) {
            if (api.varPath) {
                log.debug("Attaching ${api.class.name} at ${api.path}")
                router.attach(api.path, api).template.variables.put("identifier", new Variable(Variable.TYPE_URI_PATH))
            }
        }
    }
}
