package se.kb.libris.whelks

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
@interface IsMetadata {}

@Log
class Document extends AbstractDocument implements Resource {
    @IsMetadata
    String version = "1"
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

    Document() {
        this.timestamp = new Long(new Date().getTime())
    }

    Document(String jsonString) {
        fromJson(jsonString)
    }

    Document(File jsonFile) {
        fromJson(jsonFile)
    }

    /*
    Document(Map map) {
        fromMap(map)
    }
    */

    Document(Document d) {
        copy(d)
    }

    Document fromJson(File jsonFile) {
        try {
            Document newDoc = mapper.readValue(jsonFile, Document)
            copy(newDoc)
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
    }

    Document fromJson(String jsonString) {
        try {
            Document newDoc = mapper.readValue(jsonString, Document)
            copy(newDoc)
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
    }

    private void copy(Document d) {
        // First copy superclass fields
        copyFields(this.class.superclass, d)
        // Then copy locally declared fields
        copyFields(this.class, d)
    }

    private void copyFields(Class c, Document d) {
        c.declaredFields.each {
            if (!it.isSynthetic()
                    && !(it.getModifiers() & java.lang.reflect.Modifier.TRANSIENT)) {
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

    byte[] getData(long offset, long length) {
        byte[] ret = new byte[(int)length]
        System.arraycopy(getData(), (int)offset, ret, 0, (int)length)

        return ret
    }

    @JsonIgnore
    Date getTimestampAsDate() {
        return new Date(timestamp)
    }

    long getTimestamp() {
        return (timestamp ? timestamp.longValue() : 0L)
    }

    Document updateTimestamp() {
        timestamp = new Date().getTime()
        return this
    }

    void setTimestamp(long _t) {
        this.timestamp = _t
    }

    Document tag(String type, String value) {
        return tag(new URI(type), value)
    }

    Document tag(URI type, String value) {
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

    Document withSize(long size) {
        this.size = size
        return this
    }

    @JsonIgnore
    @Deprecated
    Map getDataAsJson() {
        return mapper.readValue(data, Map)
    }

    @JsonIgnore
    InputStream getDataAsStream() {
        return new ByteArrayInputStream(getData())
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
    String getMetadataJson() {
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

    @JsonIgnore
    InputStream getDataAsStream(long offset, long length) {
        return new ByteArrayInputStream(this.data, (int)offset, (int)length)
    }

    void untag(URI type, String value) {
        synchronized (tags) {
            Set<Tag> remove = new HashSet<Tag>()

            for (Tag t: tags)
            if (t.getType().equals(type) && t.getValue().equals(value))
                remove.add(t)

            tags.removeAll(remove)
        }
    }

    Document withLink(Link link) {
        links << link
        return this
    }
    Document withLink(String identifier) {
        links << new Link(new URI(identifier))
        return this
    }

    Document withLink(URI identifier) {
        links << new Link(identifier)
        return this
    }

    Document withLink(URI identifier, String type) {
        links << new Link(identifier, type)
        return this
    }

    Document withLink(String identifier, String type) {
        links << new Link(new URI(identifier), type)
        return this
    }

    /*
     * Methods to assist with static typing.
     */
    Document withData(byte[] data) {
        return (Document)super.withData(data)
    }

    Document withData(String data) {
        return (Document)super.withData(data)
    }
}
