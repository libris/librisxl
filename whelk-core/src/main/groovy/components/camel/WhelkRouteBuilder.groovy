package se.kb.libris.whelks.camel

import se.kb.libris.whelks.Whelk

import org.apache.camel.Processor
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary

class WhelkRouteBuilder extends RouteBuilder {

    Whelk whelk

    WhelkRouteBuilder(Whelk w) {
        this.whelk = w
    }

    void configure() {
        def le = whelk.plugins.find { it.id == "linkexpander" }
        def cu = whelk.plugins.find { it.id == "cleanupindexconverter" }
        Processor formatConverterProcessor = new FormatConverterProcessor(cu, le)

        from("direct:pairtreehybridstorage")
            .multicast()
                .to("activemq:libris.index", "activemq:libris.graphstore")

        from("activemq:libris.index")
            .process(formatConverterProcessor)
                .choice()
                    .when(header("dataset").isEqualTo("bib"))
                        .to("elasticsearch://2012M-162.local-es-cluster?ip=localhost&operation=INDEX&indexName=${whelk.id}&indexType=bib")
                    .when(header("dataset").isEqualTo("auth"))
                        .to("elasticsearch://2012M-162.local-es-cluster?ip=localhost&operation=INDEX&indexName=${whelk.id}&indexType=auth")
                    .when(header("dataset").isEqualTo("hold"))
                        .to("elasticsearch://2012M-162.local-es-cluster?ip=localhost&operation=INDEX&indexName=${whelk.id}&indexType=hold")
    }
}
