package se.kb.libris.whelks.component

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.BasicDocument
import se.kb.libris.whelks.basic.BasicPlugin
import se.kb.libris.whelks.Key
import se.kb.libris.whelks.LookupResult
import se.kb.libris.whelks.exception.*
import groovy.util.logging.Slf4j as Log
import java.net.URL
import java.net.URI
import java.net.HttpURLConnection
import java.util.concurrent.ConcurrentHashMap
import com.basho.riak.client.RiakFactory
import com.basho.riak.client.bucket.Bucket
import com.basho.riak.client.builders.RiakObjectBuilder
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.IRiakClient
import com.basho.riak.client.RiakException
import com.basho.riak.client.http.RiakObject
import com.basho.riak.client.http.response.FetchResponse
import com.basho.riak.client.http.response.RiakResponseRuntimeException
import com.basho.riak.client.raw.http.HTTPClusterConfig
import com.basho.riak.client.raw.http.HTTPClientConfig
import com.basho.riak.client.query.NodeStats
import com.basho.riak.client.cap.DefaultRetrier
import com.basho.riak.client.operations.StoreObject
import com.basho.riak.client.cap.Quora

import groovy.json.JsonSlurper
import groovy.transform.Synchronized

@Log
abstract class RiakClient extends BasicPlugin {

    static final int PING_CONNECT_TIMEOUT = 4000
    
    boolean pingNode(String host, int port){
        HttpURLConnection connection = null
        try {
            connection = (HttpURLConnection) new URL("http", host, port, "/ping").openConnection()
            connection.setRequestMethod("HEAD")
            connection.setConnectTimeout(PING_CONNECT_TIMEOUT)
            if (connection.responseCode == 200) return true
        } catch (Exception e) {
            log.info("Could not connect to node " + host)
            log.debug(e.message)
        } finally {
            if (connection != null) connection.disconnect()
        }
        return false
    }

    @Synchronized
    HTTPClusterConfig prepareClusterConfig(json_config){
        HTTPClusterConfig clusterConfig = new HTTPClusterConfig(json_config.cluster_max_connections)
        json_config.nodes.each {
            if (pingNode(it.host, it.port)) {
                HTTPClientConfig clientConfig = new HTTPClientConfig.Builder().withHost(it.host).withPort(it.port).withRiakPath(it.riak_path).withMaxConnctions(it.max_connections).withTimeout(3000).build()
                clusterConfig.addClient(clientConfig)
                log.info("Adding riak node " + it.host)
            }
        }
        log.debug("Cluster config clients: " + clusterConfig.getClients().size())
        if (clusterConfig.getClients().size() < 1) {
            throw new WhelkRuntimeException("No available Riak nodes.")
        }
        return clusterConfig
    }

    IRiakClient getClient(def json_config){
        log.info("Starting riak client using riak.json configuration...")
        return RiakFactory.newClient(prepareClusterConfig(json_config))
    }    

    IRiakClient getNodeReconfiguredClient(def json_config){
        return RiakFactory.newClient(prepareClusterConfig(json_config))
    }

    void listRiakNodes() {
        log.info("Riak client configured with nodes....")
        for (NodeStats ns : riakClient.stats()){
            log.info(ns.nodename())
        }
    }
}

@Log
class RiakStorage extends RiakClient implements Storage {
    private IRiakClient riakClient
    private ConcurrentHashMap buckets
    private String prefix
    private boolean enabled = true
    def riakjson
    static final int STORE_RETRIES = 5
    static final int DEFAULT_W_QUORUM = 1 //minimum number of responding nodes on write
    static final int DEFAULT_R_QUORUM = 1 //minimum number of responding nodes on read
    static final int DEFAULT_N_VAL = 2 //default quorum, number of replicas of stored objects
    static final boolean DEFAULT_ALLOW_MULT = false //allowing sibling objects, concurrent updates

    String id = "riakstorage"
    int order
    boolean isEnabled(){ return this.enabled }
    void enable(){ this.enabled = true }
    void disable(){ this.enabled = false }

    RiakStorage(String prefix){
        try {
            this.prefix = prefix
            riakjson = getJsonConfig()
            riakClient = getClient(riakjson)
            Bucket bucket = createBucket(prefix, DEFAULT_N_VAL, DEFAULT_ALLOW_MULT, DEFAULT_W_QUORUM, DEFAULT_R_QUORUM)
            buckets = [prefix:bucket]
            log.info("Created bucket " + prefix)
        } catch(Exception e) {
            log.debug(e.printStackTrace())
            throw new WhelkRuntimeException("Could not connect to RiakStorage. " + e.message)
        }
    }

    RiakStorage(){
        try {
            riakjson = getJsonConfig()
            riakClient = getClient(riakjson)
            riakjson.nodes.buckets.each {
                buckets[it.prefix] = createBucket(it.prefix, it.n_val, it.allow_mult, it.w, it.r)
            }
            log.info("Created buckets " + buckets.toMapString())
        } catch(Exception e) {
            log.debug(e.printStackTrace())
            throw new WhelkRuntimeException("Could not connect to RiakStorage. " + e.message)
        }
    }

    private Object getJsonConfig(){
        def is = RiakStorage.class.classLoader.getResourceAsStream("riak.json")
        return new JsonSlurper().parse(is.newReader())
    }

    private Bucket createBucket(String prefix, int n_val, boolean allow_mult, int w_quorum, int r_quorum){
        return riakClient.createBucket(prefix).nVal(n_val).allowSiblings(allow_mult).w(w_quorum).r(r_quorum).execute()
    }

    void store(Document d){
        int attempt = 0
        int loop_times = 2

        //TODO: check modified, vclock
        String key = extractIdFromURI(d.identifier)
        Bucket bucket = buckets.get(prefix)
        if (bucket == null)
            bucket = createBucket(prefix, DEFAULT_N_VAL, DEFAULT_ALLOW_MULT, DEFAULT_W_QUORUM, DEFAULT_R_QUORUM)
        while (attempt < loop_times) {
            try {
                IRiakObject riakObject = RiakObjectBuilder.newBuilder(prefix, key).withContentType(d.contentType).withValue(d.data).build()
                IRiakObject storedObject = bucket.store(riakObject).withRetrier(new DefaultRetrier(STORE_RETRIES)).execute()
                log.trace("Stored document " + d.identifier + " to riak")
                break
            } catch(RiakResponseRuntimeException rrre){
                log.warn("Could not store document with identifier " + d.identifier + " " + e.message)
                throw new WhelkRuntimeException(rrre.message)
            } catch(Exception e){
                if (attempt == loop_times-1) {
                    log.warn("Could not store document with identifier " + d.identifier + " " + e.message)
                    log.debug(e.printStackTrace())
                } else {
                    log.info("Reconfiguring riak client...")
                    riakClient = getNodeReconfiguredClient(riakjson)
                }
                attempt++
            }
        }
    }

    String extractIdFromURI(URI uri) {
        def path_fragments = uri.path.split("/")
        return path_fragments[path_fragments.length-1]
    }

    String extractBucketNameFromURI(URI uri) {
        def path_fragments = uri.path.split("/")
        return path_fragments[path_fragments.length-2]
    }

    Bucket getFetchBucket(bucket_name){
        return riakClient.fetchBucket(bucket_name).execute()
    }

    void store(Iterable<Document> docs){
        for (Document doc : docs) {
            store(doc)
        }
    }

    String createURIFromId(String id){
        return "/" + bucket_name + "/" + id
    }

    Document get(URI uri) {
        try {
            String key = extractIdFromURI(uri)
            String bucket_name = extractBucketNameFromURI(uri)
            Bucket bucket = getFetchBucket(bucket_name)
            IRiakObject riakObject = bucket.fetch(key).execute()
            return new BasicDocument().withIdentifier(key).withData(riakObject.value).withContentType(riakObject.getContentType())
        } catch (Exception e){
            log.debug("Exception trying to fetch " + uri.path + " " + e.message)
        }
        return null
    }

    Iterable<Document> getAll(){
        return new RiakIterable<Document>(this)
    }

    void delete(URI uri){
        try {
            String key = extractIdFromURI(uri)
            String bucket_name = extractBucketNameFromURI(uri)
            Bucket bucket = getFetchBucket(bucket_name)
            log.info("Deleting " + bucket_name + key)
            bucket.delete(key).execute()
       } catch (Exception e){
            log.debug("Exception trying to delete " + uri.path + " " + e.message)
            throw new RiakException("Could not delete document " + uri.path)
        }
    }

    /*boolean lookup(URI uri){
        String key = extractIdFromURI(uri)
        String bucket_name = extractBucketNameFromURI(uri)
        Bucket bucket = getFetchBucket(bucket_name)
        def keys = bucket.keys()
        bucket.keys.each {
            return it.equals(key)
        }
        return false
    }*/

    LookupResult<? extends Document> lookup(Key key){
        throw new UnsupportedOperationException("Not supported yet.")
    }
}

@Log
class RiakIterable<T> implements Iterable {

    def riakDocs

    RiakIterable(def riakStorage){
        riakDocs = new ArrayList<Document>()
        riakStorage.buckets.each {
            def prefix = it.value.name
            it.value.keys().each {
                def uri = new URI("/" + prefix + "/" + it)
                def doc = riakStorage.get(uri)
                if (doc != null) {
                    riakDocs.add(riakStorage.get(uri))
                }
            }
        }
        log.info("Loading RiakIterable for " + riakDocs.size() + " number of documents.")
    }

    Iterator<Document> iterator() {
        return riakDocs.iterator()
    }
}
