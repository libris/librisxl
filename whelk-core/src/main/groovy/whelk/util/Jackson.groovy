package whelk.util

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule

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
        static Version version = new Version(1, 0, 0, null, null, null)

        @Override
        void serialize(GString value, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
            generator.writeString(value.toString())
        }
    }
}
