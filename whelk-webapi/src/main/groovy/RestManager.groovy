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

        //tunnelService.extensionsTunnel = true
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

        log.debug("Looking for suitable APIs to attach")

        def sortedWhelks = whelks.sort { -it.contentRoot?.length() }
        
        sortedWhelks.each {
            if (it instanceof HttpWhelk) {
                def rapi = new RootRouteRestlet(it)
                log.debug("Attaching RootRoute API at ${rapi.path}")
                router.attach(rapi.path, rapi)
                def dapi = new DiscoveryAPI(it)
                log.debug("Attaching Discovery API at ${dapi.path}")
                router.attach(dapi.path, dapi)
            } 
            attachApis(router, it)
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


    void attachApis(router, whelk) {
        log.debug("Getting APIs for whelk ${whelk.id}")
        for (api in whelk.getAPIs()) {
            if (!api.varPath) {
                log.debug("Attaching strict-path-API ${api.id} at ${api.path}.")
                api.setContext(router.context.createChildContext())
                router.attach(api.path, api)
            }
        }
        for (api in whelk.getAPIs()) {
            if (api.varPath) {
                def indexType = findVar(api.path)
                log.debug("Attaching variable-path-API ${api.id} at ${api.path}. indextype $indexType")
                api.setContext(router.context.createChildContext())
                router.attach(api.path, api).template.variables.put(findVar(api.path), new Variable(Variable.TYPE_URI_PATH))
            }
        }
    }
}
