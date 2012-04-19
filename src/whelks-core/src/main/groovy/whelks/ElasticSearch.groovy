package se.kb.libris.conch

import java.util.Collection
import java.util.Map

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.elasticsearch.node.NodeBuilder.*
import org.elasticsearch.node.Node
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.WriteConsistencyLevel
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.count.CountResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.client.transport.TransportClient
//import org.elasticsearch.client.action.index.IndexRequestBuilder
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.ImmutableSettings.Builder
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentBuilder 

import org.elasticsearch.action.WriteConsistencyLevel
import se.kb.libris.whelks.Document

class ElasticSearchIndex {

    MySearch client

    def start() {
        print "Starting index thread ..."
        def th = Thread.startDaemon {
            ElasticNode node = new ElasticNode().start(ElasticNode.DATAHOME)
            node.waitForYellow()

            MySearch search = new MySearch(node.client())
            search.saveCreateIndex()       

        }
        println " ... started."
    }

    def index(Document d, def indexName, def type) {
        IndexResponse response = client.prepareIndex(indexName, type, d.identifier.toString()).setSource(jsonify(d)).execute().actionGet()
            println "Indexresponse: $response.id, $response.index, $response.type"
    }
    /*
    ElasticSearchIndex() {
    println "Initializing index"
    node = nodeBuilder().clusterName('myCluster').node()
    client = node.client()
    }


    def find(def indexName, def type, def identifier) {
    def response = client.prepareGet(indexName, type, identifier).execute().actionGet()
    return response.sourceAsString()
    }

    def jsonify(Document d) {
    return d.data
    }
    */
} 

class MySearch extends AbstractElasticSearch {

    MySearch(Client client) {
        super(client)
    }

    @Override
    public String getIndexName() {
        return "whelk"
    }

    @Override
    public String getIndexType() {
        return "marc21"
    }

}

public class ElasticNode {

    private static Logger logger = LoggerFactory.getLogger(ElasticNode.class)
    public static final String CLUSTER = "whelkcluster"
    public static final String DATAHOME = "whelkindex"
    public static final int PORT = 9300

    /*
    public static void main(String[] args) throws IOException, InterruptedException {
    ElasticNode node = new ElasticNode().start(DATAHOME)
    node.waitForYellow()

    MySearch search = new MySearch(node.client())
    search.saveCreateIndex()       

    Thread.currentThread().join()
    }
    */

    private Node node
    private boolean started = false

    public ElasticNode start(String dataHome) {
        return start(dataHome, dataHome + "/config", false)
    }

    public ElasticNode start(String home, String conf, boolean testing) {
        // see
        // http://www.elasticsearch.com/docs/elasticsearch/setup/installation/
        // http://www.elasticsearch.com/docs/elasticsearch/setup/dirlayout/
        File homeDir = new File(home)
        System.setProperty(DATAHOME+".path.home", homeDir.getAbsolutePath())
        System.setProperty(DATAHOME+".path.conf", conf)

        // increase maxClauseCount for friend search ... not necessary
        // http://wiki.apache.org/lucene-java/LuceneFAQ#Why_am_I_getting_a_TooManyClauses_exception.3F
        //        BooleanQuery.setMaxClauseCount(100000)

        Builder settings = ImmutableSettings.settingsBuilder()
        //                put("network.host", "127.0.0.1").
        //                //                put("network.bindHost", "127.0.0.0").
        //                //                put("network.publishHost", "127.0.0.0").
        //                put("index.number_of_shards", 16).
        //                put("index.number_of_replicas", 1)

        if (testing) {
            settings.put("gateway.type", "none")
            // default is local
            // none means no data after node restart!
            // does not work when transportclient connects:
            //                put("gateway.type", "fs").
            //                put("gateway.fs.location", homeDir.getAbsolutePath()).
        }

        settings.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        if (!testing) {
            nBuilder.clusterName(CLUSTER)
        } else {
            nBuilder.local(true)
        }

        node = nBuilder.build().start()

        started = true

        println "Started Node in cluster " + CLUSTER + ". Home folder: " + homeDir.getAbsolutePath()
        return this
    }

    public void stop() {
        if (node == null)
            throw new RuntimeException("Node not started")

            started = false
            node.close()
    }

    public boolean isStarted() {
        return started
    }        

    public Client client() {
        if (node == null)
            throw new RuntimeException("Node not started")

            return node.client()
    }

    /**
     * Warning: Can take several 10 seconds!
     */
    public void waitForYellow() {
        node.client().admin().cluster().health(new ClusterHealthRequest("myindex").waitForYellowStatus()).actionGet()
        logger.info("Now node status is 'yellow'!")
    }

    public void waitForOneActiveShard() {
        node.client().admin().cluster().health(new ClusterHealthRequest("myindex").waitForActiveShards(1)).actionGet()
        logger.info("Now node has at least one active shard!")
    }
}


public abstract class AbstractElasticSearch {

    private Logger logger = LoggerFactory.getLogger(getClass())
    protected Client client

    AbstractElasticSearch() {
    }

    public AbstractElasticSearch(Client client) {
        this.client = client
    }

    public AbstractElasticSearch(String url, int port) {
        client = createClient(ElasticNode.CLUSTER, url, port)
    }

    public static Client createClient(String cluster, String url, int port) {
        Settings s = ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build()
        TransportClient tmp = new TransportClient(s)
        tmp.addTransportAddress(new InetSocketTransportAddress(url, port))
        return tmp
    }

    public abstract String getIndexName()

    public abstract String getIndexType()

    public void nodeInfo() {
        NodesInfoResponse rsp = client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet()
        String str = "Cluster:" + rsp.getClusterName() + ". Active nodes:"
        str += rsp.getNodesMap().keySet()
        logger.info(str)
    }

    public boolean indexExists(String indexName) {
        // make sure node is up to create the index otherwise we get: blocked by: [1/not recovered from gateway]
        // waitForYellow()

        //        Map map = client.admin().cluster().health(new ClusterHealthRequest(indexName)).actionGet().getIndices()
        Map map = client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getIndices()
        //        System.out.println("Index info:" + map)
        return map.containsKey(indexName)
    }

    public void createIndex(String indexName) {
        // no need for the following because of _default mapping under config
        // String fileAsString = Helper.readInputStream(getClass().getResourceAsStream("tweet.json"))
        // new CreateIndexRequest(indexName).mapping(indexType, fileAsString)
        client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()
        //        waitForYellow()
    }

    public void saveCreateIndex() {
        saveCreateIndex(getIndexName(), true)
    }

    public void saveCreateIndex(String name, boolean log) {
        //         if (!indexExists(name)) {
        try {            
            createIndex(name)
            if (log)
                logger.info("Created index: " + name)
        } catch (Exception ex) {
            //        } else {
        if (log)
            logger.info("Index " + getIndexName() + " already exists")
            }
        }

    //    void ping() {
    //        waitForYellow()
    //        client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet()
    //        System.out.println("health:"+client.admin().cluster().health(new ClusterHealthRequest(getIndexName())).actionGet().getStatus().name())
    // hmmh here we need indexName again ... but in createIndex it does not exist when calling ping ...
    //        client.admin().cluster().ping(new SinglePingRequest(getIndexName(), getIndexType(), "1")).actionGet()
    //    }
    void waitForYellow() {
        waitForYellow(getIndexName())
    }

    void waitForYellow(String name) {
        client.admin().cluster().health(new ClusterHealthRequest(name).waitForYellowStatus()).actionGet()
    }

    void waitForGreen(String name) {
        client.admin().cluster().health(new ClusterHealthRequest(name).waitForGreenStatus()).actionGet()
    }

    public void refresh() {
        refresh(getIndexName())
    }

    /*
    public void refresh(Collection<String> indices) {
    refresh(Helper.toStringArray(indices))
    }
    */

    public void refresh(String... indices) {
        RefreshResponse rsp = client.admin().indices().refresh(new RefreshRequest(indices)).actionGet()
        //assertEquals(1, rsp.getFailedShards())
    }

    public long countAll() {
        CountResponse response = client.prepareCount(getIndexName()).
        setQuery(QueryBuilders.matchAllQuery()).
        execute().actionGet()
        return response.getCount()
    }

    /*
    public void feedDoc(String twitterId, XContentBuilder b) {
    //        String getIndexName() = new SimpleDateFormat("yyyyMMdd").format(tw.getCreatedAt())
    IndexRequestBuilder irb = client.prepareIndex(getIndexName(), getIndexType(), twitterId).
    setConsistencyLevel(WriteConsistencyLevel.DEFAULT).
    setSource(b)
    irb.execute().actionGet()
    }
    */

    public void deleteById(String id) {
        DeleteResponse response = client.prepareDelete(getIndexName(), getIndexType(), id).
        execute().
        actionGet()
    }

    public void deleteAll() {
        //client.prepareIndex().setOpType(OpType.)
        //there is an index delete operation
        // http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/delete_index/

        client.prepareDeleteByQuery(getIndexName()).
        setQuery(QueryBuilders.matchAllQuery()).
        execute().
        actionGet()
        refresh()
    }

    public OptimizeResponse optimize(int optimizeToSegmentsAfterUpdate) {
        return client.admin().indices().optimize(new OptimizeRequest(getIndexName()).maxNumSegments(optimizeToSegmentsAfterUpdate)).actionGet()
    }
    }
