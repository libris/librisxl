package se.kb.libris.whelks.test;

//import com.google.gson.Gson;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
/*
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig;
*/
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.WhelkManager;
import se.kb.libris.whelks.plugin.Pluggable;

public class SerialiseTest {
    public static void main(String args[]) throws MalformedURLException, IOException {
        WhelkManager manager = new WhelkManager();
        manager.registerFactory("test", new TestFactory());
        Whelk whelk = manager.createWhelk("test", "test");

        if (whelk instanceof Pluggable)
            ((Pluggable)whelk).addPlugin(new TestComponent());
        
        System.out.println(manager.serialise());

        //manager.save(new URL("file:///tmp/out.txt"));

        /*
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writer();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
        //System.out.println(mapper.writeValueAsString(manager));
        
        Gson gson = new Gson();
        gson.toJson(manager);
        gson.
        System.out.println(gson.toString());
        * 
        */
    }
}
