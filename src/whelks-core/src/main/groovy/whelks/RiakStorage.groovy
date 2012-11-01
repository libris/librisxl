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
import com.basho.riak.client.RiakFactory
import com.basho.riak.client.bucket.Bucket
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.IRiakClient
import com.basho.riak.client.RiakException
import com.basho.riak.client.http.RiakObject
import com.basho.riak.client.http.response.FetchResponse
import com.basho.riak.client.raw.http.HTTPClusterConfig
import com.basho.riak.client.raw.http.HTTPClientConfig
import com.basho.riak.client.query.NodeStats
import com.basho.riak.client.cap.DefaultRetrier

import groovy.json.JsonSlurper

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

    IRiakClient getClient(){
        log.info("Starting riak client using riak.json configuration...")
        def is = RiakClient.class.classLoader.getResourceAsStream("riak.json")
        def riakjson = new JsonSlurper().parse(is.newReader())
        HTTPClusterConfig clusterConfig = new HTTPClusterConfig(riakjson.cluster_max_connections)
        riakjson.nodes.each {
            if (pingNode(it.host, it.port)) {
                HTTPClientConfig clientConfig = new HTTPClientConfig.Builder().withHost(it.host).withPort(it.port).withRiakPath(it.riak_path).withMaxConnctions(it.max_connections).build()
                clusterConfig.addClient(clientConfig)
                log.info("Adding riak node " + it.host)
            }
        }
        return RiakFactory.newClient(clusterConfig)
    }    

    void listRiakNodes() {
        for (NodeStats ns : riakClient.stats()){
            log.info("Node: " + ns.nodename())
        }
    }
}

@Log
class RiakStorage extends RiakClient implements Storage {
    private IRiakClient riakClient
    private Bucket bucket
    private String plugin_id = "riakstore"
    private boolean enabled = true

    String getId(){ return this.clientId }
    int order
    boolean isEnabled(){ return this.enabled }
    void enable(){ this.enabled = true }
    void disable(){ this.enabled = false }

    RiakStorage(String bucket){
        try {
            riakClient = getClient()
            this.bucket = riakClient.createBucket(bucket).execute()
            log.info("Created bucket " + bucket)
        } catch(Exception e) {
            log.debug(e.printStackTrace())
            throw new WhelkRuntimeException("Could not connect to RiakStorage. " + e.message)
        }
    }

    void store(Document d){
        try {
            //TODO: check modified, vclock
            String key = extractIdFromURI(d.identifier)
            IRiakObject riakObject = bucket.store(key, d.data).withRetrier(new DefaultRetrier(1)).execute()
            log.info("Stored object with key: " + key + " to bucket: " + bucket.name)
        } catch(Exception e){
            log.debug("Exception trying to store document " + d.identifier)
            throw new RiakException("Could not store document " + d.identifier + " " + e.message)
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

    void store(Iterable<Document> d){
        for (def doc : d) {
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
            log.info("Fetched object with key" + riakObject.key)
            return new BasicDocument().withIdentifier(key).withData(riakObject.value).withContentType(riakObject.getContentType())
        } catch (Exception e){
            log.debug("Exception trying to fetch " + uri.path + " " + e.message)
            throw new RiakException("Could not fetch document " + uri.path)
        }
        return null
    }

    Iterable<Document> getAll(){
        throw new UnsupportedOperationException("Not supported yet.")
    }

    void delete(URI uri){
        try {
            String key = extractIdFromURI(uri)
            String bucket_name = extractBucketNameFromURI(uri)
            Bucket bucket = getFetchBucket(bucket_name)
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
