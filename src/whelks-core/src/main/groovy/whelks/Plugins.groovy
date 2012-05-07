package se.kb.libris.conch.plugin

import groovy.util.logging.Slf4j as Log

import org.restlet.Restlet
import org.restlet.Request
import org.restlet.Response
import org.restlet.data.MediaType

import se.kb.libris.conch.Whelk
import se.kb.libris.whelks.exception.WhelkRuntimeException


interface Plugin {
    def setWhelk(Whelk w)
}
interface API extends Plugin {
    def getPath()
    Response handle(Request request, Response response)
}

@Log
abstract class BasicWhelkAPI implements API {
    Whelk whelk
    def path

    def setWhelk(Whelk w) {
        this.whelk = w 
        this.path = "/" + (w.defaultIndex == null ? "" : w.defaultIndex + "/") + getPathEnd()
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
    Response handle(Request request, Response response) {  
        log.debug "Handled by autocomplete"
        response.setEntity("autocomplete", MediaType.TEXT_PLAIN)
        return response
    }
}

@Log
class SearchAPI extends BasicWhelkAPI {
    def pathEnd = "_find"

    @Override
    def getPath() {
        def p = super.getPath()
        return p
    }

    @Override
    Response handle(Request request, Response response) {  
        log.debug "SearchRestlet with path $path"
        def query = request.getResourceRef().getQueryAsForm().getValuesMap()
        boolean _raw = (query['_raw'] == 'true')
        try {
            def r = this.whelk.find(query.get("q"), _raw)
            response.setEntity(r, MediaType.APPLICATION_JSON)
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        }
        return response
    }
}

