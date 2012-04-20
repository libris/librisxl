package se.kb.libris.conch

import org.restlet.*
import org.restlet.data.*
import org.restlet.resource.*

import se.kb.libris.conch.*

class RestAPI extends Restlet {  

    def whelk


    RestAPI(def whelk) {
        this.whelk = whelk
    }

    def void handle(Request request, Response response) {  
        if (request.method == Method.GET) {
            response.setEntity("Hello groovy!", MediaType.TEXT_PLAIN)
        } 
        else if (request.method == Method.PUT) {
            Form form = request.getResourceRef().getQueryAsForm()
            def filename = form.getValues('filename')
            def type = form.getValues('type')
            def upload = request.entityAsText
            println "Expecting upload of file $filename of type $type"
            println "upload: ${upload}"
            def doc = new MyDocument(filename).withData(upload.getBytes('UTF-8'))
            doc.type = type
            def identifier = whelk.ingest(doc)
            response.setEntity("Thank you! Document ingested with id ${identifier}", MediaType.TEXT_PLAIN)
        }
        else {
            response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, "Unhandled request method")
        }
    }

    static main(args) {
        def env = System.getenv()
        def whelk_storage = (env["PROJECT_HOME"] ? env["PROJECT_HOME"] : System.getProperty("user.home")) + "/whelk_storage"

        DiskStorage storage = new DiskStorage(whelk_storage)
        Index index = new ElasticSearchNodeIndex()
        Whelk w = new Whelk(storage, index)
        w.name = "groovywhelk"
        RestAPI api = new RestAPI(w)
        // Create the HTTP server and listen on port 8182  
        new Server(Protocol.HTTP, 8182, api).start()
    }
}  
