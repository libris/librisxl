package whelk.integration;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http4.HttpOperationFailedException;
import whelk.integration.process.*;
import org.apache.camel.component.elasticsearch.aggregation.BulkRequestAggregationStrategy;

import whelk.filter.JsonLdLinkExpander;
import whelk.component.PostgreSQLComponent;

import java.io.IOException;
import java.util.Properties;
import java.whelk.integration.filter.FilterMessagesForAPIX;

import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

import org.apache.log4j.Logger;


public class LibrisIntegrationCamelRouteBuilder extends RouteBuilder {

    final static String VALID_CONTENTTYPE_REGEX = "application\\/(\\w+\\+)*json|application\\/x-(\\w+)-json|text/plain";

    Logger logger = Logger.getLogger(LibrisIntegrationCamelRouteBuilder.class.getName());

    private Whelk whelk = null;

    @Override
    public void configure() throws Exception {

        logger.info("Configuring camel context...");

        Properties properties = getProperties();
        String elasticCluster = properties.getProperty("elastic_cluster");
        String elasticHost = properties.getProperty("elastic_host");
        String elasticPort = properties.getProperty("elastic_port");
        String postgresqlUrl = properties.getProperty("postgresql_url");
        String postgresqlMainTable = properties.getProperty("postgresql_maintable");
        String activemqIndexQueue = properties.getProperty("activemq_es_index_queue");
        String activemqApixQueue = properties.getProperty("activemq_apix_queue");
        String activemqApixRetriesQueue = properties.getProperty("activemq_apix_retries_queue");
        String apixUrl = properties.getProperty("apixUrl");
        String apixPath = properties.getProperty("apixPath");

        whelk = new Whelk(new PostgreSQLComponent(postgresqlUrl, postgresqlMainTable));

        BulkRequestAggregationStrategy bulkRequestAggregationStrategy = new BulkRequestAggregationStrategy();
        PostgreSQLComponent postgreSQLComponent = new PostgreSQLComponent(postgresqlUrl, postgresqlMainTable);
        ElasticProcessor elasticProcessor = new ElasticProcessor(elasticCluster, elasticHost, elasticPort, new JsonLdLinkExpander(postgreSQLComponent));

        APIXProcessor apixProcessor = new APIXProcessor(apixPath, whelk, new JsonLD2MarcXMLConverter());
        APIXResponseProcessor apixResponseProcessor = new APIXResponseProcessor();
        APIXHttpResponseFailureBean apixHttpResponseFailureBean = new APIXHttpResponseFailureBean(apixResponseProcessor);

        // APIX exception handling
        onException(HttpOperationFailedException.class)
                .handled(true)
                .bean(apixHttpResponseFailureBean, "handle")
                .choice()
                .when(header("retry"))
                .to("activemq:" + activemqApixRetriesQueue)
                .otherwise()
                .end();

        // Activemq to APIX
       from("activemq:" + activemqApixQueue)
                .filter().method(FilterMessagesForAPIX.class, "isAddOrDelete")
                .filter().method(FilterMessagesForAPIX.class, "isRelevantDataset")
                .process(apixProcessor)
                .to(apixUrl)
                .process(apixProcessor)
                .choice()
                .when(header("retry"))
                .to(activemqApixRetriesQueue)
                .otherwise()
                .end();


        // Activemq retry to APIX
        from("activemq:" + activemqApixRetriesQueue)
                .delay(60000)
                .routingSlip(header("next"));


        // Activemq to Elasticsearch
//        from("activemq:" + activemqIndexQueue)
//                .filter(header("document:contentType").regex(VALID_CONTENTTYPE_REGEX))
//                .process(elasticProcessor)
//                .aggregate(header("document:dataset"), bulkRequestAggregationStrategy).completionSize(2000).completionTimeout(5000)
//                .routingSlip(header("elasticDestination"));
        //.to("elasticsearch:local?operation=INDEX&indexName=test&indexType=test");


//        from("activemq:" + activemqApixQueue)
//                .process(new APIXTestProcessor());

//        from(activemqIndexQueue)
//                .process(new APIXProcessor(apixPath, whelk))
//                .to("elasticsearch:local?operation=INDEX&indexName=test&indexType=test");


    }

    private Properties getProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("integration.properties"));
        return properties;
    }
}
