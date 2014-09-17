package se.kb.libris.whelks.component.support

import groovy.util.logging.Slf4j as Log

import org.apache.jena.fuseki.server.FusekiConfig
import org.apache.jena.fuseki.server.SPARQLServer
import org.apache.jena.fuseki.server.ServerConfig

import com.hp.hpl.jena.sparql.core.DatasetGraph
import com.hp.hpl.jena.tdb.TDBFactory

import se.kb.libris.whelks.plugin.BasicPlugin

@Log
class FusekiServer extends BasicPlugin {

    FusekiServer() {
        log.info("Starting a Fuseki server.")

        def datasetPath = "/libris"

        DatasetGraph dsg = TDBFactory.createDatasetGraph() ;
        ServerConfig config = FusekiConfig.defaultConfiguration(datasetPath, dsg, true, true) ;

        config.port = 3030

        SPARQLServer server = new SPARQLServer(config)
        server.start()
    }
}
