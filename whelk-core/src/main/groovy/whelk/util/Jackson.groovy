package whelk.util

import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.JsonProcessingException
import org.codehaus.jackson.Version
import org.codehaus.jackson.map.JsonSerializer
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.SerializerProvider
import org.codehaus.jackson.map.module.SimpleModule

class Jackson {
    public static final ObjectMapper mapper = mapper()
    
    static ObjectMapper mapper() {
        ObjectMapper mapper = new ObjectMapper()
        def module = new SimpleModule(GStringSerializer.class.getSimpleName(), GStringSerializer.version)
        module.addSerializer(GString, new GStringSerializer())
        mapper.registerModule(module)
        return mapper
    }

    static class GStringSerializer extends JsonSerializer<GString> {
        static Version version = new Version(1, 0, 0, null)
        
        @Override
        void serialize(GString value, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
            generator.writeString(value.toString())
        }
    }
}
