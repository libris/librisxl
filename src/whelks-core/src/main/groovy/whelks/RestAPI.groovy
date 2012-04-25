package se.kb.libris.conch

import org.restlet.Restlet
import org.restlet.Request
import org.restlet.Response
import org.restlet.Server
import org.restlet.data.Form
import org.restlet.data.MediaType
import org.restlet.data.Method
import org.restlet.data.Protocol

import com.google.gson.Gson

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.conch.*
import se.kb.libris.conch.component.*

class ServiceApplication extends Application {

    ServiceApplication(Context parentContext) {
        super(parentContext)
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx) {
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
                responseHeaders.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                responseHeaders.add("Access-Control-Allow-Credentials", "false")
            }
        }

        router.attach("/",
        new Redirector(ctx, "{rh}/ui/", Redirector.MODE_CLIENT_SEE_OTHER))
        router.attach("/ui",
        new Redirector(ctx, "{rh}/ui/", Redirector.MODE_CLIENT_SEE_OTHER))

        router.attach("/collector", new Finder(ctx, RDFLoaderHandler))

        router.attach("/view", new SparqlTreeRouter(ctx, components.repository))

        router.attach("/var/terms",
        new DataFinder(ctx, components.repository,
        components.jsonLdSettings, components.dataAppBaseUri + "var/terms",
        "/sparql/construct_terms.rq"))

        router.attach("/var/common",
        new DataFinder(ctx, components.repository,
        components.jsonLdSettings, components.dataAppBaseUri + "var/common",
        "/sparql/construct_common.rq"))

        router.attach("/{path}/data",
        new DataFinder(ctx, components.repository,
        components.jsonLdSettings, components.dataAppBaseUri,
        "/sparql/construct_relrev_data.rq")
        ).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))

        router.attach("/{path}/index",
        new DataFinder(ctx, components.repository,
        components.jsonLdSettings, components.dataAppBaseUri,
        "/sparql/construct_summary.rq")
        ).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))

        if (components.elasticQuery) {
            router.attach("/-/{docType}",
            new ElasticFinder(ctx, components.elasticQuery))
        }

        if (mediaDirUrl) {
            router.attach("/json-ld/", new Directory(ctx, "clap:///json-ld/"))
            router.attach("/css/", new Directory(ctx, mediaDirUrl + "css/"))
            router.attach("/img/", new Directory(ctx, mediaDirUrl + "img/"))
            router.attach("/js/", new Directory(ctx, mediaDirUrl + "js/"))
            router.attach("/ui", new Directory(ctx, mediaDirUrl + "ui"))
        }
        return router
    }

}

class RestAPI extends Restlet {  

    def whelk


    RestAPI(def whelk) {
        this.whelk = whelk
    }

    def void handle(Request request, Response response) {  
        if (request.method == Method.GET) {
            println "Request path: ${request.resourceRef.path}"
            def path = request.resourceRef.path
            def query = request.getResourceRef().getQueryAsForm().getValuesMap()
            if (path == "/_find") {
                def r = whelk.find(query.get("q"))
                response.setEntity(r, MediaType.APPLICATION_JSON)
            } else if (path != "/") { 
                def d = whelk.retrieve(path)
                if (d == null) {
                    Map<String, String> responsemap = new HashMap<String, String>()
                    Gson gson = new Gson()
                    responsemap.put("status", "error")
                    def reason = "No document with identifier ${path}"
                    responsemap.put("reason", reason)
                    response.setEntity(gson.toJson(responsemap), MediaType.APPLICATION_JSON) 
                } else {
                    response.setEntity(new String(d.data), MediaType.APPLICATION_JSON)
                }
            }
            else {
                response.setEntity("Try a URI for a document, or /_find?q=query to search", MediaType.TEXT_PLAIN)
            }
        }
        else if (request.method == Method.PUT) {
            upload(request, response)
        } 
        else if (request.method == Method.POST) {
            // New document without identifier
            upload(request, response)
        }
        else {
            response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, "Unhandled request method")
        }
    }

    def upload(Request request, Response response) {
        def upload = request.entityAsText
        try {
            def path = request.resourceRef.path
            def doc
            if (path == "/") {
                doc = new MyDocument().withData(upload.getBytes('UTF-8'))
            } else {
                doc = new MyDocument(path).withData(upload.getBytes('UTF-8'))
            }
            def identifier = whelk.ingest(doc)
                response.setEntity("Thank you! Document ingested with id ${identifier}\n", MediaType.TEXT_PLAIN)
        } catch (WhelkRuntimeException wre) {
            response.setEntity(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
        }
    }

    static main(args) {
        Whelk w = new Whelk("whelk")

        w.addComponent(new DiskStorage())
        w.addComponent(new ElasticSearchNode())

        RestAPI api = new RestAPI(w)
        //
        // Create the HTTP server and listen on port 8182  
        new Server(Protocol.HTTP, 8182, api).start()
    }
}  
