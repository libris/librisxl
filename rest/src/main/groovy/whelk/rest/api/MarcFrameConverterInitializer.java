package whelk.rest.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.converter.marc.MarcFrameConverter;
import whelk.util.WhelkFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class MarcFrameConverterInitializer implements ServletContextListener {
    private static final Logger log = LogManager.getLogger(MarcFrameConverterInitializer.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        initMarcFrameConverterAsync();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        //
    }

    void initMarcFrameConverterAsync() {
        new Thread(() -> {
            try {
                WhelkFactory.getSingletonWhelk().getMarcFrameConverter();
                log.info("Started {}", MarcFrameConverter.class.getSimpleName());
            }
            catch (Exception e) {
                log.warn("Error starting {}: {}", MarcFrameConverter.class.getSimpleName(), e, e);
            }
        }).start();
    }
}
