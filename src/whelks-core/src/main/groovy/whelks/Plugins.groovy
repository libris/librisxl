package se.kb.libris.conch.plugin

import groovy.util.logging.Slf4j as Log

import org.restlet.Request
import org.restlet.Response

import se.kb.libris.conch.Whelk
import se.kb.libris.conch.SearchRestlet


interface Plugin {
    def setWhelk(Whelk w)
}
interface API extends Plugin {
    def getPath()
}

class AutoComplete extends SearchRestlet implements API {

    def path = "/author/_complete"
    Whelk whelk

    AutoComplete(Whelk whelk) {
        super(whelk)
    }

    def setWhelk(Whelk w) { this.whelk = w }

    def void handle(Request request, Response response) {  
        println "Handled by autocomplete"
        request.attributes["path"] = "/author"
        super.handle(request, response)
    }
}
