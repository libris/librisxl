package whelk.camel

import groovy.util.logging.Slf4j as Log

import javax.xml.bind.JAXBException

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.builder.impl.*
import org.apache.camel.view.ModelFileGenerator

@Log
public class WhelkCamelMain extends MainSupport {

    CamelContext camelContext = new DefaultCamelContext();

    WhelkCamelMain() {
        this.camelContexts.add(camelContext)
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart()
        camelContext.start()
    }

    void addComponent(String name, Component component ) {
        this.camelContext.addComponent(name, component);
    }

    void addRoutes(RouteBuilder rb) {
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
