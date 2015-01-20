package whelk.camel

import groovy.util.logging.Slf4j as Log

import javax.xml.bind.JAXBException

import whelk.camel.route.*

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.builder.impl.*
import org.apache.camel.view.ModelFileGenerator

@Log
public class WhelkCamelMain extends MainSupport {

    CamelContext camelContext = new DefaultCamelContext();

    String addQueueUri, bulkAddQueueUri, removeQueueUri

    WhelkCamelMain(String aquri, String baquri, rquri) {
        this.camelContexts.add(camelContext)
        this.addQueueUri = aquri
        this.bulkAddQueueUri = baquri
        this.removeQueueUri = rquri
    }

    @Override
    protected void doStart() throws Exception {
        constructMasterRoute()
        super.doStart()
        camelContext.start()
    }

    void addComponent(String name, Component component ) {
        this.camelContext.addComponent(name, component);
    }

    void addRoutes(RouteBuilder rb) {
        addRouteBuilder(rb)
        camelContext.addRoutes(rb)
    }

    protected ModelFileGenerator createModelFileGenerator() throws JAXBException {
        return null;
    }

    @Override
    protected  Map<String, CamelContext> getCamelContextMap() {
        return ['main': camelContext]
    }

    @Override
    protected ProducerTemplate findOrCreateCamelTemplate() {
        return camelContext.createProducerTemplate()
    }

    void enableHangupSupport() {
        HangupInterceptor interceptor = new HangupInterceptor(this);
        Runtime.getRuntime().addShutdownHook(interceptor);
    }

    void constructMasterRoute() {
        def addMQs = routeBuilders.findAll { it instanceof WhelkRouteBuilderPlugin && it.messageQueue }*.messageQueue
        def bulkAddMQs = routeBuilders.findAll { it instanceof WhelkRouteBuilderPlugin && it.bulkMessageQueue }*.bulkMessageQueue
        def removeMQs = routeBuilders.findAll { it instanceof WhelkRouteBuilderPlugin && it.removeQueue }*.removeQueue

        addRoutes(new RouteBuilder() {
            @Override
            void configure() {
                if (addQueueUri && addMQs.size() > 0) {
                    log.debug("multicasting $addMQs to $addQueueUri")
                    from(addQueueUri).multicast().parallelProcessing().to(addMQs as String[])
                }
                if (bulkAddQueueUri && bulkAddMQs.size() > 0) {
                    log.debug("multicasting $bulkAddMQs to $bulkAddQueueUri")
                    from(bulkAddQueueUri).multicast().parallelProcessing().to(bulkAddMQs as String[])
                }
                if (removeQueueUri && removeMQs.size() > 0) {
                    log.debug("multicasting $removeMQs to $removeQueueUri")
                    from(removeQueueUri).multicast().parallelProcessing().to(removeMQs as String[])
                }
            }
        })
    }
}

/**
 * A class for intercepting the hang up signal
 * and do a graceful shutdown of the Camel.
 * Taken from the camel-spring implementation of MainSupport.
 */
@Log
class HangupInterceptor extends Thread {

    MainSupport mainInstance;

    public HangupInterceptor(MainSupport main) {
        mainInstance = main;
    }

    @Override
    public void run() {
        try {
            mainInstance.stop();
        } catch (Exception ex) {
            log.info("Shutdown handler").warn(ex);
        }
    }
}
