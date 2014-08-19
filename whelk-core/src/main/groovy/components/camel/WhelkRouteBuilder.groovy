package se.kb.libris.whelks.camel

import org.apache.camel.builder.RouteBuilder

class WhelkRouteBuilder extends RouteBuilder {

    void configure() {
        from("direct:storage")
        .to("activemq:libris")

        /*
        from("activemq:libris")
        .to("elasticsearch://local?operation=INDEX&indexName=test&indexType=message")
        */
    }
}
