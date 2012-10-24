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
import com.basho.riak.client.http.RiakClient
import com.basho.riak.client.RiakException
import com.basho.riak.client.http.RiakObject
import com.basho.riak.client.http.response.FetchResponse

@Log
class RiakStorage extends BasicPlugin implements Storage {
    private RiakClient riakClient
    private byte[] clientId
    private String bucket_name
    private int riak_port
    private String plugin_id = "riakstore"
    private boolean enabled = true

    String getId(){ return this.clientId }
    int order
    boolean isEnabled(){ return this.enabled }
    void enable(){ this.enabled = true }
    void disable(){ this.enabled = false }

    RiakStorage(String bucket){
        try {
            Properties properties = new Properties()
            def propInputStream = RiakStorage.class.getClassLoader().getResourceAsStream("whelks-core.properties")
            properties.load(propInputStream)
            def riak_host = properties.getProperty("riakhost")
            def riak_port = Integer.parseInt(properties.getProperty("riakport"))
            def url = "http://" + riak_host + ":" + riak_port + "/riak/"
            bucket_name = bucket
            riakClient = new RiakClient(url)
            clientId = riakClient.clientId
            log.info("Loading RiakClient " + url)
         } catch(RiakException re) {
            log.debug("Could not initialize riak httpclient for: " + url + " " + e.getMessage())
            throw new WhelkRuntimeException("Could not connect to RiakStorage. Url: " + url + " " + e.printStackTrace())
        }
    }

    void store(Document d){
        String key = extractIdFromURI(d.identifier)
        RiakObject riakObject = new RiakObject(riakClient, bucket_name, key, d.data, d.contentType)
        def storeResponse = riakClient.store(riakObject)
        log.info("Stored object with key: " + key + " to bucket: " + bucket_name + " response status: " + storeResponse.getStatusCode())
    }

    String extractIdFromURI(URI uri) {
        def path_fragments = uri.path.split("/")
        return path_fragments[path_fragments.length-1]
    }

    String extractBucketNameFromURI(URI uri) {
        def path_fragments = uri.path.split("/")
        return path_fragments[path_fragments.length-2]
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
        String key = extractIdFromURI(uri)
        String bucket = extractBucketNameFromURI(uri)
        FetchResponse fetchResponse = riakClient.fetch(bucket, key)
        if (fetchResponse.getStatusCode().toString().trim().substring(0,1) == "2") {
            return new BasicDocument().withIdentifier(key).withData(fetchResponse.getObject().getValue()).withContentType(fetchResponse.getObject().getContentType())
        }
        return null
    }

    Iterable<Document> getAll(){
        throw new UnsupportedOperationException("Not supported yet.")
    }

    void delete(URI uri){
        String key = extractIdFromURI(uri)
        String bucket = extractBucketNameFromURI(uri)
        FetchResponse response = new FetchResponse(riakClient.delete(bucket, key))
    }

    LookupResult<? extends Document> lookup(Key key){
        throw new UnsupportedOperationException("Not supported yet.")
    }
}
