package se.kb.libris.whelks.camel

import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.plugin.Plugin
import se.kb.libris.whelks.plugin.WhelkAware

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary
import org.apache.camel.processor.aggregate.*

class WhelkRouteBuilder extends RouteBuilder implements WhelkAware {

    // Properties copied from BasicPlugin
    String id = null
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map global

    Whelk whelk

    void configure() {
        Processor formatConverterProcessor = getPlugin("elastic_camel_processor")
        assert formatConverterProcessor

        from("direct:pairtreehybridstorage")
            .multicast()
                .to("activemq:libris.index", "activemq:libris.graphstore")

        from("activemq:libris.index")
            .process(formatConverterProcessor)
                .routingSlip("elasticDestination")
    }

    // Plugin methods
    @Override
    public void init(String initString) {}
    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }

    public Plugin getPlugin(String pluginId) {
        return plugins.find { it.id == pluginId }
    }
}

class ArrayListAggregationStrategy implements AggregationStrategy {

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        Object newBody = newExchange.getIn().getBody()
        ArrayList<Object> list = null
        if (oldExchange == null) {
            list = new ArrayList<Object>()
            list.add(newBody)
            newExchange.getIn().setBody(list)
            return newExchange
        } else {
            list = oldExchange.getIn().getBody(ArrayList.class)
            list.add(newBody)
            return oldExchange
        }
    }
}
