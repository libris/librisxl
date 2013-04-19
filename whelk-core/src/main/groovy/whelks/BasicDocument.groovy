package se.kb.libris.whelks.basic

import groovy.transform.TypeChecked
import groovy.util.logging.Slf4j as Log

import java.io.*
import java.net.URI
import java.util.*
import java.nio.ByteBuffer
import java.lang.annotation.*

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*


@Target(value = ElementType.FIELD)
@Retention(value = RetentionPolicy.RUNTIME)
public @interface IsMetadata {}

@Log
public class BasicDocument implements Document {
    @IsMetadata
    URI identifier

    @IsMetadata
    String version = "1", contentType

    byte[] data

    @IsMetadata
    long size

    @IsMetadata
    Set<Link> links = new HashSet<Link>()

    @IsMetadata
    Set<Tag> tags = new HashSet<Tag>()

    //@IsMetadata
    Set<Description> descriptions = new TreeSet<Description>()

    @IsMetadata
    long timestamp = 0

    @JsonIgnore
    ObjectMapper mapper = new ElasticJsonMapper()

    public BasicDocument() {
        this.timestamp = new Long(new Date().getTime())
    }

    public BasicDocument(String jsonString) {
        fromJson(jsonString)
    }

    public BasicDocument(File jsonFile) {
        fromJson(jsonFile)
    }

    /*
    public BasicDocument(Map map) {
        fromMap(map)
    }
    */

    public BasicDocument(Document d) {
        copy(d)
    }

    public Document fromJson(File jsonFile) {
        try {
            BasicDocument newDoc = mapper.readValue(jsonFile, BasicDocument)
            copy(newDoc)
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
    }

    public Document fromJson(String jsonString) {
        try {
            BasicDocument newDoc = mapper.readValue(jsonString, BasicDocument)
            copy(newDoc)
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
    }

    private void copy(Document d) {
        this.class.declaredFields.each {
            if (!it.isSynthetic() && !(it.getModifiers() & java.lang.reflect.Modifier.TRANSIENT)) {
                this.(it.name) = d.(it.name)
            }
        }
    }

    String toJson() {
        return mapper.writeValueAsString(this)
    }

    Map toMap() {
        return mapper.convertValue(this, Map)
    }

    @Override
    public byte[] getData() {
        return data
    }

    @Override
    public byte[] getData(long offset, long length) {
        byte[] ret = new byte[(int)length]
        System.arraycopy(getData(), (int)offset, ret, 0, (int)length)

        return ret
    }

    @Override
    public long getSize() {
        return (size ? size.longValue() : 0L)
    }

    @Override
    @JsonIgnore
    public Date getTimestampAsDate() {
        return new Date(timestamp)
    }

    @Override 
    public long getTimestamp() {
        return (timestamp ? timestamp.longValue() : 0L)
    }

    @Override
    public Document updateTimestamp() {
        timestamp = new Date().getTime()
        return this
    }

    @Override
    public void setTimestamp(long _t) {
        this.timestamp = _t
    }

    public Document tag(String type, String value) {
        return tag(new URI(type), value)
    }

    @Override
    public Document tag(URI type, String value) {
        /*
        synchronized (tags) {
            for (Tag t: tags)
            if (t.getType().equals(type) && t.getValue().equals(value))
                return t
        }
        */
        Tag tag = new Tag(type, value)

        this.tags.add(tag)

        return this
    }

    @Override
    public Document withData(String dataString) {
        return withData(dataString.getBytes("UTF-8"))
    }

    @Override
    public Document withIdentifier(String uri) {
        try {
            this.identifier = new URI(uri)
        } catch (java.net.URISyntaxException e) {
            throw new WhelkRuntimeException(e)
        }
        return this
    }

    @Override
    public Document withIdentifier(URI uri) {
        this.identifier = uri
        return this
    }

    @Override
    public Document withData(byte[] data) {
        this.data = data
        this.size = data.length
        return this
    }

    @Override
    public Document withContentType(String contentType) {
        this.contentType = contentType
        return this
    }

    @Override
    public Document withSize(long size) {
        this.size = size
        return this
    }

    @Override
    @JsonIgnore
    public String getDataAsString() {
        return new String(getData())
    }

    @Override
    @JsonIgnore
    public Map getDataAsJson() {
        return mapper.readValue(data, Map)
    }

    @Override
    @JsonIgnore
    public InputStream getDataAsStream() {
        return new ByteArrayInputStream(getData())
    }

    @JsonIgnore
    public Map getDataAsJsonMap() {
        def jsonmap = [:]
        this.class.declaredFields.each {
            if (this.(it.name) && !it.isSynthetic() && !(it.getModifiers() & java.lang.reflect.Modifier.TRANSIENT)) {
                if (this.(it.name) instanceof URI) {
                    log.trace("found a URI identifier")
                    jsonmap[it.name] = this.(it.name).toString()
                } else if (it.type.isArray()) {
                    log.trace("Found a bytearray")
                    def l = []
                    l.addAll(0, this.(it.name))
                    jsonmap[it.name] = l
                } else {
                    log.trace("default writing ${it.name}")
                    jsonmap[it.name] = this.(it.name)
                }
            }
        }
        log.trace "JsonMap: $jsonmap"
        return jsonmap
    }

    @JsonIgnore
    String getMetadataAsJson() {
        def elements = []
        def fields = this.class.declaredFields.findAll {
            !it.synthetic &&
            it.getModifiers() != java.lang.reflect.Modifier.TRANSIENT &&
            it.getAnnotation(IsMetadata.class) != null
        }
        for (field in fields) {
            field.setAccessible(true)
            if (Set.class.isAssignableFrom(field.type)) {
                def l = field.get(this).collect {
                    it.toJson()
                }
                elements.add("\"${field.name}\":[" + l.join(",")+"]")
            } else {
                String value = field.get(this)?.toString()
                if (value) {
                    try {
                        elements.add("\"${field.name}\":"+field.getLong(this))
                    } catch (Exception e) {
                        elements.add("\"${field.name}\":\""+value+"\"")
                    }
                }
            }
        }
        return "{"+elements.join(",")+"}"
    }

    /*
    @JsonIgnore
    public String getMetadataJson() {
        def mapper = new ObjectMapper()
        def out = this.class.declaredFields.findAll {
            !it.synthetic &&
            it.getModifiers() != java.lang.reflect.Modifier.TRANSIENT &&
            it.getAnnotation(IsMetadata.class) != null
        }.collectEntries { v ->
            log.trace("v.type " + v.type)
            if (v.type == "interface java.util.Set") {
                [(v.name) : getObjectAsJson(this[v.name])]
            } else {
                [(v.name) : this[v.name]]
            }
        }
        log.trace("Constructed metadatajson: $out")
        return mapper.writeValueAsString(out)
    }
    */

    String getObjectAsJson(def obj) {
        def map = mapper.convertValue(obj, Map)
        return mapper.writeValueAsString(map)
    }

    @Override
    @JsonIgnore
    public InputStream getDataAsStream(long offset, long length) {
        return new ByteArrayInputStream(getData(), (int)offset, (int)length)
    }

    @Override
    public void untag(URI type, String value) {
        synchronized (tags) {
            Set<Tag> remove = new HashSet<Tag>()

            for (Tag t: tags)
            if (t.getType().equals(type) && t.getValue().equals(value))
                remove.add(t)

            tags.removeAll(remove)
        }
    }

    @Override
    Document withLink(Link link) {
        links << link
        return this
    }
    @Override
    Document withLink(String identifier) {
        links << new Link(new URI(identifier))
        return this
    }

    @Override
    Document withLink(URI identifier) {
        links << new Link(identifier)
        return this
    }

    @Override
    Document withLink(URI identifier, String type) {
        links << new Link(identifier, type)
        return this
    }

    @Override
    Document withLink(String identifier, String type) {
        links << new Link(new URI(identifier), type)
        return this
    }
}

@Log
class HighlightedDocument extends BasicDocument {
    Map<String, String[]> matches = new TreeMap<String, String[]>()

    HighlightedDocument(Document d, Map<String, String[]> match) {
        withData(d.getData()).withIdentifier(d.identifier).withContentType(d.contentType)
        this.matches = match
    }

    @Override
    String getDataAsString() {
        def mapper = new ElasticJsonMapper()
        def json = mapper.readValue(super.getDataAsString(), Map)
        json.highlight = matches
        return mapper.writeValueAsString(json)
    }
}
