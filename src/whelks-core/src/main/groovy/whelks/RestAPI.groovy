package se.kb.libris.conch

import groovy.util.logging.Slf4j as Log

import org.restlet.Application
import org.restlet.Context
import org.restlet.Restlet
import org.restlet.Request
import org.restlet.Response
import org.restlet.Server
import org.restlet.data.Disposition
import org.restlet.data.Form
import org.restlet.data.MediaType
import org.restlet.data.Method
import org.restlet.data.Protocol
import org.restlet.data.Status
import org.restlet.representation.Representation
import org.restlet.routing.Router
import org.restlet.routing.Variable
import org.restlet.ext.servlet.ServerServlet
import com.google.gson.Gson

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.conch.*
import se.kb.libris.conch.component.*
import se.kb.libris.conch.data.*
import se.kb.libris.conch.plugin.*

@Log
class ServiceApplication extends Application {

    boolean allowCORS = true
    def whelks = []

    ServiceApplication(Context parentContext) {
        super(parentContext)
        def allwhelk = new Whelk("all")
        allwhelk.defaultIndex = null
        def bibwhelk = new Whelk("bib")
        def authwhelk = new Whelk("author")
        // Try using only ElasticSearch as storage
        //whelk.addComponent(new DiskStorage())
        def es = new ElasticSearchClient()
        // Using same es backend for all whelks
        allwhelk.addPlugin(es)
        allwhelk.addPlugin(new SearchAPI())
        authwhelk.addPlugin(es)
        authwhelk.addPlugin(new AutoComplete())
        authwhelk.addPlugin(new SearchAPI())
        bibwhelk.addPlugin(es)
        bibwhelk.addPlugin(new SearchAPI())
        whelks.add(allwhelk)
        whelks.add(bibwhelk)
        whelks.add(authwhelk)
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx)

        def docRestlet = new DocumentRestlet()
        docRestlet.setWhelks(whelks)

        router.attach("/", new Restlet() {
            void handle(Request request, Response response) {
                if (request.method == Method.POST) {
                    // Handle uploaded unnamed document
                }
                response.setEntity("Try a URI for a document, or ${request.rootRef}/_find?q=query to search", MediaType.TEXT_PLAIN)
            }
        })

        log.debug("Look for suitable APIs to attach")
        for (whelk in whelks) {
            for (api in whelk.getApis()) {
                log.debug("Attaching ${api.class.name} at ${api.path}")
                router.attach(api.path, new APIWrapper(api))
            }
        }

        log.debug("Attaching standard routes")

        /*
        router.attach("/_find", searchRestlet)
        router.attach("{path}/_find", searchRestlet).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))
        */
        router.attach("{path}", docRestlet).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))
        
        return router
    }
}

@Log 
class APIWrapper extends Restlet {
    API api

    APIWrapper(API a) {
        this.api = a
    }

    def void handle(Request request, Response response) {  
        try {
            response = api.handle(request, response)
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        }
    }

}

@Log
abstract class WhelkRestlet extends Restlet {
    def whelks = [:]

    def setWhelks(w) {
        w.each {
            this.whelks[it.name] = it
        }
    }


    Whelk lookupWhelk(path) {
        def pathparts = (path == null ? [] : path.split("/"))
        if (pathparts.size() < 2) {
            // No sensible whelk selected. Use the first one (TODO?)
            def whelk = whelks.values().toList()[0]
            log.debug("No Whelk specified, using ${whelk.name}")
            return whelk
        }
        def wname = pathparts[1]
        def whelk = whelks[wname]
        if (whelk == null) {
            throw new WhelkRuntimeException("No such Whelk ($wname)")
        }
        log.debug("Using Whelk ${whelk.name}")
        return whelk
    }
}

@Log
class DocumentRestlet extends WhelkRestlet {

    def void handle(Request request, Response response) {
        String path = request.attributes["path"]
        boolean _raw = (request.getResourceRef().getQueryAsForm().getValuesMap()['_raw'] == 'true')
        if (request.method == Method.GET) {
            log.debug "Request path: ${path}"
            try {
                def d = lookupWhelk(path).retrieve(path, _raw)
                response.setEntity(new String(d.data), new MediaType(d.contentType))
            } catch (WhelkRuntimeException wrte) {
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
            }
        }
        else if (request.method == Method.PUT || request.method == Method.POST) {
            Representation r = request.entity
            Disposition d = r.disposition
            if (d != null) {
                println "Filename: ${d.filename}"
                println "Type: ${d.type}"
            }
            println "Size: ${r.size}"
            println "Mediatype: ${r.mediaType} " + r.mediaType.toString()
            def is = r.stream
            byte[] data = new byte[r.size]
            int i = 0
            is.eachByte { b ->
                data[i++] = b
            }

            def upload = new String(data)
            println "UPLOAD:\n" + upload + "\n--------------------"
            try {
                def doc
                if (path == "/") {
                    doc = new MyDocument().withData(data)
                } else {
                    doc = new MyDocument(path).withData(data)
                }
                def identifier = lookupWhelk(path).ingest(doc)
                response.setEntity("Thank you! Document ingested with id ${identifier}\n", MediaType.TEXT_PLAIN)
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
    }
}  
