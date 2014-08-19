package se.kb.libris.whelks.camel

import org.apache.camel.*
import org.apache.camel.impl.*

class DiskStorageConsumer extends DefaultConsumer {

    Endpoint diskEndpoint

    DiskStorageConsumer(Endpoint endpoint, Processor processor) {
        super(endpoint, processor)
        this.diskEndpoint = endpoint
    }

    @Override
    Endpoint getEndpoint() {
        return diskEndpoint
    }
}

