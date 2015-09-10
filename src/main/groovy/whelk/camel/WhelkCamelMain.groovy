package whelk.camel

import groovy.util.logging.Slf4j as Log

import javax.xml.bind.JAXBException

import whelk.camel.route.*

import org.apache.camel.CamelContext
import org.apache.camel.Component
import org.apache.camel.ProducerTemplate
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.main.MainSupport
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.view.ModelFileGenerator

@Log
public class WhelkCamelMain extends MainSupport {

    CamelContext camelContext = new DefaultCamelContext();

    String queueUri

    WhelkCamelMain(String quri) {
        this.camelContexts.add(camelContext)
        this.queueUri = quri
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
        def MQs = routeBuilders.findAll { it instanceof WhelkRouteBuilderPlugin && it.messageQueue }*.messageQueue

        addRoutes(new RouteBuilder() {
            @Override
            void configure() {
                if (queueUri && MQs.size() > 0) {
                    log.debug("multicasting $MQs to $queueUri")
                    from(queueUri).multicast().parallelProcessing().to(MQs as String[])
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
