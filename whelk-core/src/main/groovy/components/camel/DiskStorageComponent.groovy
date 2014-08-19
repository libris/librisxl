package se.kb.libris.whelks.camel

import groovy.util.logging.Slf4j as Log

import org.apache.camel.*
import org.apache.camel.impl.*

@Log
class DiskStorageComponent extends UriEndpointComponent {

    DiskStorageComponent() {
        super(DiskStorageEndpoint.class)
    }

    DiskStorageComponent(CamelContext ctx) {
        super(ctx, DiskStorageEndpoint.class)
    }

    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        Endpoint endpoint = new DiskStorageEndpoint(uri, this, parameters);
        setProperties(endpoint, parameters);
        return endpoint;
    }
}
