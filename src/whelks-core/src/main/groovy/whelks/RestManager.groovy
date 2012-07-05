package se.kb.libris.conch

import java.util.regex.Pattern 
import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.representation.*
import org.restlet.routing.*

import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.*
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.plugin.*

@Log
class RestManager extends Application {

    final String WHELKCONFIGFILE = "/tmp/whelkconfig.json"
    //WhelkManager manager
    def whelks = []

    RestManager(Context parentContext) {
        super(parentContext)
        log.debug("Using file encoding: " + System.getProperty("file.encoding"));
        init()
    }

    void init() {
        def bibwhelk = new WhelkImpl("bib")
        def authwhelk = new WhelkImpl("auth")
        def suggestwhelk = new WhelkImpl("suggest")

        // Add storage and index
        bibwhelk.addPlugin(new ElasticSearchClient(bibwhelk.prefix))
        authwhelk.addPlugin(new ElasticSearchClient(authwhelk.prefix))
        suggestwhelk.addPlugin(new ElasticSearchClient(suggestwhelk.prefix))
        //suggestwhelk.addPlugin(new InMemoryStorage())

        // Add APIs
        bibwhelk.addPlugin(new SearchRestlet())
        bibwhelk.addPlugin(new DocumentRestlet())
        authwhelk.addPlugin(new SearchRestlet())
        authwhelk.addPlugin(new DocumentRestlet())
        authwhelk.addPlugin(new LogRestlet())
        suggestwhelk.addPlugin(new SearchRestlet())
        suggestwhelk.addPlugin(new DocumentRestlet())
        def acplugin = new AutoComplete()
        acplugin.addNamePrefix("100.a")
        acplugin.addNamePrefix("400.a")
        acplugin.addNamePrefix("500.a")
        suggestwhelk.addPlugin(acplugin)

        // Add other plugins 
        def formatParameters = ["script": "sug_json.py", "bibwhelk": bibwhelk, "suggestwhelk": suggestwhelk]
        
        suggestwhelk.addPlugin(new Listener(bibwhelk, 5, PythonRunnerFormatConverter.class, formatParameters))
        suggestwhelk.addPlugin(new Listener(authwhelk, 5, PythonRunnerFormatConverter.class, formatParameters))

        whelks << bibwhelk
        whelks << authwhelk
        whelks << suggestwhelk

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

        whelks.each {
            log.debug("Getting APIs for whelk ${it.prefix}")
            for (api in it.getAPIs()) {
                if (!api.varPath) {
                    router.attach(api.path, api)
                }
            }
            for (api in it.getAPIs()) {
                log.debug("Attaching ${api.class.name} at ${api.path}")
                if (api.varPath) {
                    router.attach(api.path, api).template.variables.put("path", new Variable(Variable.TYPE_URI_PATH))
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
