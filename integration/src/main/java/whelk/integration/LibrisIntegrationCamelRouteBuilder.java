package whelk.integration;

import org.apache.camel.builder.RouteBuilder;
import whelk.integration.process.APIXProcessor;
import whelk.integration.process.ElasticProcessor;
import org.apache.camel.component.elasticsearch.aggregation.BulkRequestAggregationStrategy;

import whelk.filter.JsonLdLinkExpander;
import whelk.component.PostgreSQLComponent;

import java.io.IOException;
import java.util.Properties;

public class LibrisIntegrationCamelRouteBuilder extends RouteBuilder {

    final static String VALID_CONTENTTYPE_REGEX = "application\\/(\\w+\\+)*json|application\\/x-(\\w+)-json|text/plain";

    @Override
    public void configure() throws Exception {

        Properties properties = getProperties();
        String elasticCluster = properties.getProperty("elastic_cluster");
        String elasticHost = properties.getProperty("elastic_host");
        String elasticPort = properties.getProperty("elastic_port");
        String postgresqlUrl = properties.getProperty("postgresql_url");
        String postgresqlMainTable = properties.getProperty("postgresql_maintable");
        String activemqIndexQueue = properties.getProperty("activemq_es_index_queue");
        String activemqApixQueue = properties.getProperty("activemq_apix_queue");
        String activemqApixRetriesQueue = properties.getProperty("activemq_apix_retries_queue");
        String apixUri = properties.getProperty("apix_Uri");

        BulkRequestAggregationStrategy bulkRequestAggregationStrategy = new BulkRequestAggregationStrategy();
        PostgreSQLComponent postgreSQLComponent = new PostgreSQLComponent(postgresqlUrl, postgresqlMainTable);
        ElasticProcessor elasticProcessor = new ElasticProcessor(elasticCluster, elasticHost, elasticPort, new JsonLdLinkExpander(postgreSQLComponent));
        APIXProcessor apixProcessor = new APIXProcessor("PREFIX??");

        // Exception handling
        onException(Exception.class)
                .handled(true)
                .transform().simple("Error reported: ${exception.message} - cannot process this message.");

        // Activemq to Elasticsearch
        from(activemqIndexQueue)
                .filter(header("document:contentType").regex(VALID_CONTENTTYPE_REGEX))
                //.process()
                .aggregate(header("document:dataset"), bulkRequestAggregationStrategy).completionSize(2000).completionTimeout(5000)
                .routingSlip(header("elasticDestination"));
                //.to("elasticsearch:local?operation=INDEX&indexName=test&indexType=test");

        // Activemq to APIX
        from(activemqApixQueue)
                .filter("groovy", "['ADD', 'DELETE'].contains(request.getHeader('whelk:operation'))")
                .filter("groovy", "['auth','bib','hold'].contains(request.getHeader('document:dataset'))") // Only save auth hold and bib
                .process(apixProcessor)
                .to(apixUri)
                .process(apixProcessor)
                .choice()
                .when(header("retry"))
                .to(activemqApixRetriesQueue)
                .otherwise()
                .end();

        from(activemqApixQueue)
                .process(new APIXProcessor("https://libris.kb.se/apix/0.1/cat/test/"))
                .to("elasticsearch:local?operation=INDEX&indexName=test&indexType=test");

        // TODO: Activemq retries to APIX
    }

    private Properties getProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("integration.properties"));
        return properties;
    }
}
