package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import whelk.converter.marc.MarcFrameConverter
import whelk.util.WhelkFactory

import javax.servlet.ServletContextEvent
import javax.servlet.ServletContextListener

@Log
class MarcFrameConverterInitializer implements ServletContextListener {

    @Override
    void contextInitialized(ServletContextEvent sce) {
        initMarcFrameConverterAsync()
    }

    @Override
    void contextDestroyed(ServletContextEvent sce) {
        //
    }

    void initMarcFrameConverterAsync() {
        new Thread(new Runnable() {
            void run() {
                try {
                    WhelkFactory.getSingletonWhelk().getMarcFrameConverter()
                    log.info("Started ${MarcFrameConverter.class.getSimpleName()}")
                }
                catch (Exception e) {
                    log.warn("Error starting ${MarcFrameConverter.class.getSimpleName()}: $e", e)
                }
            }
        }).start()
    }
}
