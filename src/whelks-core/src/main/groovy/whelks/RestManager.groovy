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
import se.kb.libris.whelks.WhelkImpl
import se.kb.libris.whelks.api.RestAPI
import se.kb.libris.whelks.component.DiskStorage
import se.kb.libris.whelks.plugin.PythonRunnerFormatConverter

@Log
class RestManager extends Application {

    def whelks = [:]

    RestManager(Context parentContext) {
        super(parentContext)
        def allwhelk = new WhelkImpl(this, "all")
        allwhelk.defaultIndex = null
        def bibwhelk = new WhelkImpl(this, "bib")
        def authwhelk = new WhelkImpl(this, "author")
        def suggestwhelk = new WhelkImpl(this, "suggest")
        // Try using only ElasticSearch as storage
        //whelk.addComponent(new DiskStorage())
        def es = new ElasticSearchClient()
        def ds = new DiskStorage()
        def suggest_conv = new PythonRunnerFormatConverter("/Users/Markus/dev/java/librisxl/src/whelks-core/src/main/resources/sug_json.py")
        // Using same es backend for all whelks
        allwhelk.addPlugin(es)
        authwhelk.addPlugin(es)
        //authwhelk.addPlugin(ds)
        bibwhelk.addPlugin(es)
        suggestwhelk.addPlugin(es)
        //bibwhelk.addPlugin(ds)
        allwhelk.addPlugin(new SearchRestlet())
        authwhelk.addPlugin(new AutoComplete())
        authwhelk.addPlugin(new SearchRestlet())
        authwhelk.addPlugin(new DocumentRestlet())
        suggestwhelk.addPlugin(new SearchRestlet())
        suggestwhelk.addPlugin(new DocumentRestlet())
        suggestwhelk.addPlugin(suggest_conv)
        suggestwhelk.listenTo(authwhelk)
        bibwhelk.addPlugin(new SearchRestlet())
        bibwhelk.addPlugin(new DocumentRestlet())
        whelks.put(allwhelk.name, allwhelk)
        whelks.put(bibwhelk.name, bibwhelk)
        whelks.put(authwhelk.name, authwhelk)
        whelks.put(suggestwhelk.name, suggestwhelk)
    }

    /**
     * Very simple method to resolve the correct document given a URI.
     */
    Document resolve(URI uri) {
        return whelks[uri.toString().split("/")[1]].get(uri)
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

        whelks.each { 
            for (api in it.value.getApis()) {
                log.debug("Attaching ${api.class.name} at ${api.path}")
                if (api.varPath) {
                    log.debug("Setting varpath")
                    router.attach(api.path, api).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))
                } else {
                    router.attach(api.path, api)
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
    def varPath = false
    boolean enabled = true

    String id = "RestAPI"
    
    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    @Override
    def void setWhelk(Whelk w) {
        this.whelk = w 
        def tmppath = (w.defaultIndex == null ? "" : w.defaultIndex) + getModifiedPathEnd()
        this.path = (tmppath.startsWith("/") ? tmppath : "/" + tmppath)
    }

    private String getModifiedPathEnd() {
        if (getPathEnd() == null || getPathEnd().length() == 0) {
            return ""
        } else {
            return "/" + getPathEnd()
        }
    }

    /*
    def getVarPath() {
        return this.path.matches(Pattern.compile("\\{\\w\\}.+"))
    }
    */

}

@Log
class DocumentRestlet extends BasicWhelkAPI {

    def pathEnd = "{path}"
    def varPath = true

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
        log.debug("reqattr path: " + request.attributes["path"])
        String path = path.replaceAll(_escape_regex(pathEnd), request.attributes["path"])
        //path = request.attributes["path"]
        boolean _raw = (request.getResourceRef().getQueryAsForm().getValuesMap()['_raw'] == 'true')
        if (request.method == Method.GET) {
            log.debug "Request path: ${path}"
            try {
                def d = whelk.get(path, _raw)
                println "Got document with ctype: ${d.contentType}"
                response.setEntity(new String(d.data), new MediaType(d.contentType))
            } catch (WhelkRuntimeException wrte) {
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
            }
        }
        else if (request.method == Method.PUT || request.method == Method.POST) {
            try {
                def identifier 
                Document doc = null
                if (path == "/") {
                    doc = this.whelk.createDocument().withContentType(request.entity.mediaType.toString()).withSize(request.entity.size).withDataAsStream(request.entity.stream)
                } else {
                    doc = this.whelk.createDocument().withIdentifier(new URI(path)).withContentType(request.entity.mediaType.toString()).withSize(request.entity.size).withDataAsStream(request.entity.stream)
                }
                identifier = this.whelk.store(doc)
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
            def r = this.whelk.query(query.get("q"), _raw)
            response.setEntity(r.result, MediaType.APPLICATION_JSON)
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
