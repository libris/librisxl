package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.imports.*

interface RestAPI extends API {
    String getPath()
}

@Log
abstract class BasicWhelkAPI extends Restlet implements RestAPI {
    Whelk whelk

    def pathEnd = ""
    def varPath = false
    boolean enabled = true

    String id = "RestAPI"
    
    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    @Override
    def void setWhelk(Whelk w) {
        this.whelk = w 
    }

    String getPath() {
        return "/" + this.whelk.name + "/" + getPathEnd()
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
                response.setEntity(d.dataAsString, new MediaType(d.contentType))
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
                    doc = this.whelk.createDocument().withIdentifier(new URI(path)).withContentType(request.entity.mediaType.toString()).withData(request.entityAsText)
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
        try {
            def results = this.whelk.query(query.get("q"))
            if (results.numberOfHits > 0) {
                response.setEntity(results.toJson(), MediaType.APPLICATION_JSON)
            } else {
                response.setEntity("No results for query", MediaType.TEXT_PLAIN)
            }
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        }
    }
}

@Log
class BibSearchRestlet extends BasicWhelkAPI {

    def pathEnd = "_bibsearch"

    @Override
    def void handle(Request request, Response response) {
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
class ImportRestlet extends BasicWhelkAPI {
    def pathEnd = "_import"

    BatchImport importer
    ImportRestlet() {
        importer = new BatchImport()
    }

    @Override
    def void handle(Request request, Response response) {
        importer.resource = this.whelk.name
        importer.manager = this.whelk.manager
        def millis = System.currentTimeMillis()
        def count = importer.doImport()
        def diff = (System.currentTimeMillis() - millis)/1000
        response.setEntity("Imported $count records into ${whelk.name} in $diff seconds.\n", MediaType.TEXT_PLAIN)
    }
}

@Log 
class AutoComplete extends BasicWhelkAPI {

    def pathEnd = "_complete"
}
