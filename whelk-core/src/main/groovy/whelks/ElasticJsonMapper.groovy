package se.kb.libris.whelks.basic

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.ser.CustomSerializerFactory
import org.elasticsearch.common.text.*

class ElasticJsonMapper extends ObjectMapper {
    ElasticJsonMapper() {
        super();
        CustomSerializerFactory sf = new CustomSerializerFactory();
        this.setSerializerFactory(sf);

        sf.addGenericMapping(StringAndBytesText.class, new JsonSerializer<StringAndBytesText>() {
            @Override
            public void serialize(StringAndBytesText value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
                jgen.writeObject(value.string())
            }
        });
        sf.addGenericMapping(StringText.class, new JsonSerializer<StringText>() {
            @Override
            public void serialize(StringText value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
                jgen.writeObject(value.string())
            }
        });
    }
}
