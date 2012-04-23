package se.kb.libris.conch

import org.restlet.Restlet
import org.restlet.Request
import org.restlet.Response
import org.restlet.Server
import org.restlet.data.Form
import org.restlet.data.MediaType
import org.restlet.data.Method
import org.restlet.data.Protocol

import se.kb.libris.whelks.Document
import se.kb.libris.conch.*
import se.kb.libris.conch.component.*

class RestAPI extends Restlet {  

    def whelk


    RestAPI(def whelk) {
        this.whelk = whelk
    }

    def void handle(Request request, Response response) {  
        if (request.method == Method.GET) {
            def query = request.getResourceRef().getQueryAsForm().getValuesMap()
            if (query.containsKey("load")) {
                def d = whelk.retrieve(query.get("load"))
                println "Loaded something from whelk : $d"
                response.setEntity(new String(d.data), MediaType.APPLICATION_JSON)
            } 
            else if (query.containsKey("find")) {
                whelk.find(query.get("find"))
                response.setEntity("Find in index", MediaType.TEXT_PLAIN)
            } 
            else {
                response.setEntity("Hello groovy!", MediaType.TEXT_PLAIN)
            }
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
            doc.index = whelk.name
            def identifier = whelk.ingest(doc)
            response.setEntity("Thank you! Document ingested with id ${identifier}", MediaType.TEXT_PLAIN)
        }
        else {
            response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, "Unhandled request method")
        }
    }

    static main(args) {
        Whelk w = new Whelk("whelk")

        w.addComponent(new DiskStorage())
        w.addComponent(new ElasticSearchNodeIndex())

        RestAPI api = new RestAPI(w)
        //
        // Create the HTTP server and listen on port 8182  
        new Server(Protocol.HTTP, 8182, api).start()
    }
}  
