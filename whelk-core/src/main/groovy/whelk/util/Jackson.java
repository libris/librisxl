package whelk.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import groovy.lang.GString;

import java.io.IOException;

public class Jackson {
    public static final ObjectMapper mapper = mapper();

    public static ObjectMapper mapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule(GStringSerializer.class.getSimpleName(), GStringSerializer.version);
        module.addSerializer(GString.class, new GStringSerializer());
        mapper.registerModule(module);
        return mapper;
    }

    public static class GStringSerializer extends JsonSerializer<GString> {
        static final Version version = new Version(1, 0, 0, null, null, null);

        @Override
        public void serialize(GString value, JsonGenerator generator, SerializerProvider provider) throws IOException {
            generator.writeString(value.toString());
        }
    }
}
