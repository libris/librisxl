package se.kb.libris.whelks.camel

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.RuntimeCamelException;

@UriEndpoint(scheme = "diskstorage")
class DiskStorageEndpoint extends DefaultEndpoint {

    /*
    @UriParam
    private DiskStorageConfiguration configuration;
    */

    public DiskStorageEndpoint(String uri, DiskStorageComponent component, Map<String, Object> parameters) throws Exception {
        super(uri, component)
        //this.configuration = new DiskStorageConfiguration(new URI(uri), parameters)
    }

    public Producer createProducer() throws Exception {
        throw new RuntimeCamelException("Cannot produce to a disk storage: " + getEndpointUri());
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new DiskStorageConsumer(this, processor)
    }

    public boolean isSingleton() {
        return true;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        /*
        if (configuration.isLocal()) {
            LOG.info("Starting local ElasticSearch server");
        } else {
            LOG.info("Joining ElasticSearch cluster " + configuration.getClusterName());
        }
        if (configuration.getIp() != null) {
            LOG.info("REMOTE ELASTICSEARCH: {}", configuration.getIp());
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", configuration.getClusterName())
                    .put("client.transport.ignore_cluster_name", false)
                    .put("node.client", true)
                    .put("client.transport.sniff", true)
                    .build();
            Client client = new TransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(configuration.getIp(), configuration.getPort()));
            this.client = client;
        } else {
            node = configuration.buildNode();
            client = node.client();
        }
        */
    }

    @Override
    protected void doStop() throws Exception {
        /*
        if (configuration.isLocal()) {
            LOG.info("Stopping local ElasticSearch server");
        } else {
            LOG.info("Leaving ElasticSearch cluster " + configuration.getClusterName());
        }
        client.close();
        if (node != null) {
            node.close();
        }
        */
        super.doStop();
    }

    /*
    public DiskStorageConfiguration getConfig() {
        return configuration;
    }
    */
}
