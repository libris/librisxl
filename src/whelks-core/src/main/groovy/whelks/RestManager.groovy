package se.kb.libris.conch

import java.util.regex.Pattern 
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
import se.kb.libris.whelks.component.ElasticSearchClient
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.Whelk
import se.kb.libris.conch.data.MyDocument
/*
import se.kb.libris.conch.*
import se.kb.libris.conch.component.*
import se.kb.libris.conch.data.*
*/
import se.kb.libris.whelks.api.RestAPI

@Log
class RestManager extends Application {

    boolean allowCORS = true
    def whelks = []

    RestManager(Context parentContext) {
        super(parentContext)
        def allwhelk = new WhelkImpl("all")
        allwhelk.defaultIndex = null
        def bibwhelk = new WhelkImpl("bib")
        def authwhelk = new WhelkImpl("author")
        // Try using only ElasticSearch as storage
        //whelk.addComponent(new DiskStorage())
        def es = new ElasticSearchClient()
        // Using same es backend for all whelks
        allwhelk.addPlugin(es)
        authwhelk.addPlugin(es)
        bibwhelk.addPlugin(es)
        allwhelk.addAPI(new SearchRestlet())
        authwhelk.addAPI(new AutoComplete())
        authwhelk.addAPI(new SearchRestlet())
        authwhelk.addAPI(new DocumentRestlet())
        bibwhelk.addAPI(new SearchRestlet())
        bibwhelk.addAPI(new DocumentRestlet())
        whelks.add(allwhelk)
        whelks.add(bibwhelk)
        whelks.add(authwhelk)
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx)


        router.attach("/", new Restlet() {
            void handle(Request request, Response response) {
                if (request.method == Method.POST) {
                    // Handle uploaded unnamed document
                }
                response.setEntity("Try a URI for a document, or ${request.rootRef}/_find?q=query to search", MediaType.TEXT_PLAIN)
            }
        })

        log.debug("Looking for suitable APIs to attach")
        for (whelk in whelks) {
            for (api in whelk.getApis()) {
                log.debug("Attaching ${api.class.name} at ${api.path}")
                if (api.varPath) {
                    router.attach(api.path, api).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))
                } else {
                    router.attach(api.path, api)
                }
            }
        }

        return router
    }
}

@Log 
class APIWrapper extends Restlet {
    RestAPI api

    APIWrapper(RestAPI a) {
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
abstract class BasicWhelkAPI extends Restlet implements RestAPI {
    Whelk whelk
    def path
    def pathEnd = ""
    boolean enabled = true
    
    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    @Override
    def void setWhelk(Whelk w) {
        this.whelk = w 
        this.path = "/" + (w.defaultIndex == null ? "" : w.defaultIndex) + getModifiedPathEnd()
    }

    def getModifiedPathEnd() {
        if (getPathEnd() == null || getPathEnd().length() == 0) {
            return ""
        } else {
            return "/" + getPathEnd()
        }
    }

    def getVarPath() {
        return this.path.matches(Pattern.compile("\\{\\w\\}.+"))
    }

}

@Log
class DocumentRestlet extends BasicWhelkAPI {

    def pathEnd = "{path}"

    def _escape_regex(str) {
        def escaped = new StringBuffer()
        str.each {
            if (it == "{" || it == "}") {
                escaped << "\\"
            }
            escaped << it 
        }
        return escaped.toString()
    }

    def void handle(Request request, Response response) {
        String path = path.replaceAll(_escape_regex(pathEnd), request.attributes["path"])
        boolean _raw = (request.getResourceRef().getQueryAsForm().getValuesMap()['_raw'] == 'true')
        if (request.method == Method.GET) {
            log.debug "Request path: ${path}"
            try {
                def d = whelk.retrieve(path, _raw)
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
                def identifier = this.whelk.ingest(doc)
                response.setEntity("Thank you! Document ingested with id ${identifier}\n", MediaType.TEXT_PLAIN)
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
    }
}  

@Log
class SearchRestlet extends BasicWhelkAPI {

    def pathEnd = "_find"

    @Override
    def void handle(Request request, Response response) {
        log.debug "SearchRestlet with path $path"
        def query = request.getResourceRef().getQueryAsForm().getValuesMap()
        boolean _raw = (query['_raw'] == 'true')
        try {
            def r = this.whelk.find(query.get("q"), _raw)
            response.setEntity(r, MediaType.APPLICATION_JSON)
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        }
    }
}

@Log 
class AutoComplete extends BasicWhelkAPI {

    def pathEnd = "_complete"

    @Override
    def getPath() {
        def p = super.getPath()
        return p
    }

    @Override
    def action() {  
        log.debug "Handled by autocomplete"
    }
}
