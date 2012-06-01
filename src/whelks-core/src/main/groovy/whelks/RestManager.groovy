package se.kb.libris.conch

import java.util.regex.Pattern 
import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.representation.*
import org.restlet.routing.*

import se.kb.libris.whelks.component.ElasticSearchClient
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.*
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.plugin.PythonRunnerFormatConverter

@Log
class RestManager extends Application {

    final String WHELKCONFIGFILE = "/tmp/whelkconfig.json"
    WhelkManager manager

    RestManager(Context parentContext) {
        super(parentContext)
        System.out.println("Using file encoding: " + System.getProperty("file.encoding"));
        init()
    }

    void init() {
        if (new File(WHELKCONFIGFILE).exists()) {
            log.debug("Found serialised configuration. Trying to bootstrap ...")
            manager = new WhelkManager(new URL("file://${WHELKCONFIGFILE}"))
            log.debug("Manager bootstrapped. Contains "+manager.whelks.size()+" whelks ...")
            manager.whelks.each {
                log.debug("Say hello whelk ${it.key}: ${it.value}")
            }
        } else {
            log.debug("Virgin installation. Setting up some whelks.")
            manager = new WhelkManager()

            def bibwhelk = manager.addWhelk(new WhelkImpl(), "bib")
            def authwhelk = manager.addWhelk(new WhelkImpl(), "auth")
            def suggestwhelk = manager.addWhelk(new WhelkImpl(), "suggest")

            // Add storage and index
            def es = new ElasticSearchClient()
            bibwhelk.addPlugin(es)
            authwhelk.addPlugin(es)
            suggestwhelk.addPlugin(es)

            // Add APIs
            bibwhelk.addPlugin(new SearchRestlet())
            bibwhelk.addPlugin(new ImportRestlet())
            bibwhelk.addPlugin(new DocumentRestlet())
            authwhelk.addPlugin(new SearchRestlet())
            authwhelk.addPlugin(new ImportRestlet())
            authwhelk.addPlugin(new DocumentRestlet())
            suggestwhelk.addPlugin(new SearchRestlet())
            suggestwhelk.addPlugin(new DocumentRestlet())

            // Add other plugins (formatconverters et al)
            suggestwhelk.addPlugin(new PythonRunnerFormatConverter("sug_json.py"))

            manager.establishListening("auth", "suggest")
            manager.establishListening("bib", "suggest")

            manager.save(new URL("file://${WHELKCONFIGFILE}"))
        }
    }


    /**
     * Very simple method to resolve the correct document given a URI.
     */
    Document resolve(URI uri) {
        return manager.whelks[uri.toString().split("/")[1]].get(uri)
    }

    @Override
    synchronized Restlet createInboundRoot() {
        def ctx = getContext()
        def router = new Router(ctx)


        router.attach("/", new Restlet() {
            void handle(Request request, Response response) {
                if (request.method == Method.POST) {
                    // TODO: Handle uploaded unnamed document
                }
                response.setEntity("Try a URI for a document, or ${request.rootRef}/_find?q=query to search", MediaType.TEXT_PLAIN)
            }
        })

        log.debug("Looking for suitable APIs to attach")

        manager.whelks.each { 
            log.debug("Manager found whelk ${it.key}")
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

