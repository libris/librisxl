package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*

import org.json.simple.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.imports.*
import se.kb.libris.whelks.persistance.*

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
        return "/" + this.whelk.prefix + "/" + getPathEnd()
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
            def q = query.get("q")
            def callback = query.get("callback")
            if (q) {
                def results = this.whelk.query(query.get("q"))
                def jsonResult = 
                    (callback ? callback + "(" : "") +
                    results.toJson() +
                    (callback ? ");" : "") 

                response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
            } else {
                response.setEntity("Missing q parameter", MediaType.TEXT_PLAIN)
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
            def r = this.whelk.query(query.get("q"))
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
class AutoComplete extends BasicWhelkAPI implements JSONSerialisable, JSONInitialisable {

    def pathEnd = "_complete"

    def namePrefixes = []

    def void addNamePrefix(String prefix) {
        namePrefixes << prefix
    }

    @Override
    def void handle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        String name = querymap.get("name")
        if (!name) {
            name = querymap.get("q")
        }
        def callback = querymap.get("callback")
        if (name) {
            if (name[-1] != ' ' && name[-1] != '*') {
                name = name + "*"
            }
            def names = []
            namePrefixes.each { p ->
                def nameparts = []
                name.split(" ").each {
                    nameparts << "$p:$it"
                }
                names << nameparts.join(" AND ")
            }
            //def query = names.join(" OR ")
            LinkedHashMap sortby = new LinkedHashMap<String,String>()
            //sortby['100.a'] = "asc"
            sortby['records'] = "desc"
            def query = new Query(name)
            query.highlights = namePrefixes
            query.sorting = sortby
            query.fields = namePrefixes

            def results = this.whelk.query(query)
            def jsonResult = 
                (callback ? callback + "(" : "") +
                results.toJson() +
                (callback ? ");" : "") 

            response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
        } else {
            response.setEntity('{"error":"Parameter \"name\" is missing."}', MediaType.APPLICATION_JSON)
        }
    }

    @Override
    public JSONInitialisable init(JSONObject obj) {
        for (Iterator it = obj.get("prefixes").iterator(); it.hasNext();) {
            try {
                def _prefix = it.next();
                namePrefixes << _prefix.toString()
            } catch (Exception e) {
                throw new WhelkRuntimeException(e);
            }
        }
        
        return this;
    }

    @Override
    JSONObject serialize() {
        JSONObject _api = new JSONObject();
        _api.put("_classname", this.getClass().getName());
        
        JSONArray _prefixes = new JSONArray();
        namePrefixes.each {
            _prefixes.add(it)
        }
        _api.put("prefixes", _prefixes);
                
        return _api;
    }
}
