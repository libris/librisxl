package se.kb.libris.conch

import groovy.util.logging.Slf4j as Log

import org.restlet.Application
import org.restlet.Context
import org.restlet.Restlet
import org.restlet.Request
import org.restlet.Response
import org.restlet.Server
import org.restlet.data.Form
import org.restlet.data.MediaType
import org.restlet.data.Method
import org.restlet.data.Protocol
import org.restlet.data.Status
import org.restlet.routing.Router
import org.restlet.routing.Variable
import org.restlet.ext.servlet.ServerServlet
import com.google.gson.Gson

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.conch.*
import se.kb.libris.conch.data.*
import se.kb.libris.conch.component.*

@Log
class ServiceApplication extends Application {

    boolean allowCORS = true
    Whelk whelk

    ServiceApplication(Context parentContext) {
        super(parentContext)
        whelk = new Whelk("whelk")
        // Try using only ElasticSearch as storage
        //whelk.addComponent(new DiskStorage())
        whelk.addComponent(new ElasticSearchNode())
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx)
        /*{
            @Override
            void handle(Request request, Response response) {
                if (allowCORS) {
                    addCORSHeaders(response)
                }
                super.handle(request, response)
            }
            private addCORSHeaders(Response response) {
                def responseHeaders = response.attributes.get("org.restlet.http.headers")
                if (responseHeaders == null) {
                    responseHeaders = new Form()
                    response.attributes.put("org.restlet.http.headers", responseHeaders)
                }
                responseHeaders.add("Access-Control-Allow-Origin", "*")
                responseHeaders.add("Access-Control-Allow-Methods", "GET, POST, PUT")
                responseHeaders.add("Access-Control-Allow-Credentials", "false")
            }
        }*/

        def docRestlet = new DocumentRestlet(whelk)
        def searchRestlet = new SearchRestlet(whelk)

        router.attach("/", new Restlet() {
            void handle(Request request, Response response) {
                if (request.method == Method.POST) {
                    // Handle uploaded unnamed document
                }
                response.setEntity("Try a URI for a document, or ${request.rootRef}/_find?q=query to search", MediaType.TEXT_PLAIN)
            }
        })

        //router.attach("/{path}/_find", new SearchRestlet(whelk))
        router.attach("/_find", searchRestlet)
        router.attach("{path}", docRestlet).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))
        
        return router
    }
}

abstract class WhelkRestlet extends Restlet {
    def whelk

    WhelkRestlet(def whelk) {
        this.whelk = whelk
    }
}

@Log
class SearchRestlet extends WhelkRestlet {  

    SearchRestlet(Whelk whelk) {
        super(whelk)
    }

    def void handle(Request request, Response response) {  
        def path = request.resourceRef.path
        def query = request.getResourceRef().getQueryAsForm().getValuesMap()
        def r = whelk.find(query.get("q"))
        response.setEntity(r, MediaType.APPLICATION_JSON)
    }
}


@Log
class DocumentRestlet extends WhelkRestlet {  

    DocumentRestlet(Whelk whelk) {
        super(whelk)
    }

    def void handle(Request request, Response response) {  
        final String path = request.attributes["path"]
        if (request.method == Method.GET) {
            log.debug "Request path: ${path}"
            def d = whelk.retrieve(path)
            if (d == null) {
                Map<String, String> responsemap = new HashMap<String, String>()
                Gson gson = new Gson()
                responsemap.put("status", "error")
                responsemap.put("reason", "No document with identifier " + path)
                response.setEntity(gson.toJson(responsemap), MediaType.APPLICATION_JSON) 
            } else {
                response.setEntity(new String(d.data), MediaType.APPLICATION_JSON)
            }
        }
        else if (request.method == Method.PUT || request.method == Method.POST) {
            def upload = request.entityAsText
            try {
                def doc
                if (path == "/") {
                    doc = new MyDocument().withData(upload.getBytes())
                } else {
                    doc = new MyDocument(path).withData(upload.getBytes())
                }
                def identifier = whelk.ingest(doc)
                    response.setEntity("Thank you! Document ingested with id ${identifier}\n", MediaType.TEXT_PLAIN)
            } catch (WhelkRuntimeException wre) {
                response.setEntity(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
    }

    def correctPath(Request req) {

    }

    /*
    static main(args) {
        Whelk w = new Whelk("whelk")

        w.addComponent(new DiskStorage())
        w.addComponent(new ElasticSearchNode())

        RestAPI api = new RestAPI(w)
        //
        // Create the HTTP server and listen on port 8182  
        new Server(Protocol.HTTP, 8182, api).start()
    }
    */
}  
