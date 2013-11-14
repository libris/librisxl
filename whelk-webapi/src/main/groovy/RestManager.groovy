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

        tunnelService.extensionsTunnel = false
        metadataService.addExtension("ttl", MediaType.APPLICATION_RDF_TURTLE)
        metadataService.addExtension("jsonld", new MediaType("application/ld+json"))

        log.debug("Using file encoding: " + System.getProperty("file.encoding"));
        log.debug("Retrievieng whelks from JNDI ...")
        whelks = new javax.naming.InitialContext().lookup("whelks")
        log.debug("Found these whelks: $whelks")
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx)
        def allAPIs

        log.debug("Looking for suitable APIs to attach")

        def sortedwhelks = whelks.sort( {a, b -> b.contentRoot.length() <=> a.contentRoot.length() } )

        sortedwhelks.each {  whelk ->
            log.debug("Getting APIs for whelk ${whelk.id}")

            allAPIs = whelk.getAPIs()

            if (whelk instanceof HttpWhelk) {
                allAPIs << new RootRouteRestlet(whelk)
                allAPIs << new DiscoveryAPI(whelk)
            }

            router = attachApis(router, allAPIs)
        }
        return router
    }

    def findVar(varPath) {
        def varStr = new StringBuffer()
        def varBegin = false
        def varEnd = false
        for (it in varPath) {
            switch (it) {
                case "{":
                    varBegin = true
                    break
                case "}":
                    varEnd = false
                    break
                default:
                    if (varBegin && !varEnd) {
                        varStr << it
                    }
            }
        }
        return varStr.toString()
    }

    Router attachApis(router, apis) {
        def var
        for (api in apis) {
            api.setContext(router.context.createChildContext())
            if (api.varPath) {
                var = findVar(api.path)
                log.debug("Attaching variable-path-API ${api.id} at ${api.path}. Putting uri-path variable $var")
                router.attach(api.path, api).template.variables.put(var, new Variable(Variable.TYPE_URI_PATH))
            } else {
               log.debug("Attaching strict-path-API ${api.id} at ${api.path}.")
               router.attach(api.path, api)
            }
        }
        return router
    }
}
