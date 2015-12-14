package whelk.integration;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http.HttpOperationFailedException; //or http4.HttpOperationFailedException?
import org.apache.camel.component.elasticsearch.aggregation.BulkRequestAggregationStrategy;

import whelk.Whelk;
import whelk.component.ElasticSearch;
import whelk.component.Index;
import whelk.component.PostgreSQLComponent;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.filter.JsonLdLinkExpander;
import whelk.integration.process.*;
import whelk.integration.filter.FilterMessagesForAPIX;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;


public class LibrisIntegrationCamelRouteBuilder extends RouteBuilder {

    Logger logger = Logger.getLogger(LibrisIntegrationCamelRouteBuilder.class.getName());

    PostgreSQLComponent postgreSQLComponent = null;
    Index elastic = null;

    private Whelk whelk = null;

    @Override
    public void configure() throws Exception {

        logger.info("Configuring camel context...");

        Properties properties = getProperties();
        String postgresqlUrl = properties.getProperty("postgresql_url");
        String postgresqlMainTable = properties.getProperty("postgresql_maintable");
        String activemqApixQueue = properties.getProperty("activemq_apix_queue");
        String activemqApixRetriesQueue = properties.getProperty("activemq_apix_retries_queue");
        String apixUrl = properties.getProperty("apixUrl");
        String apixPath = properties.getProperty("apixPath");

        postgreSQLComponent = new PostgreSQLComponent(postgresqlUrl, postgresqlMainTable);

        whelk = new Whelk(postgreSQLComponent, elastic);

        APIXProcessor apixProcessor = new APIXProcessor(apixPath, whelk, new JsonLD2MarcXMLConverter());
        APIXResponseProcessor apixResponseProcessor = new APIXResponseProcessor(whelk);
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
                .to("activemq:" + activemqApixRetriesQueue)
                .otherwise()
                .end();


        // Activemq retry to APIX
        from("activemq:" + activemqApixRetriesQueue)
                .delay(60000)
                .routingSlip(header("next"));


    }

    private Properties getProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("integration.properties"));
        return properties;
    }
}
