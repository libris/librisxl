package se.kb.libris.whelks.test;

import java.net.MalformedURLException;
import java.net.URL;
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.WhelkManager;
import se.kb.libris.whelks.plugin.Pluggable;

public class SerialiseTest {
    public static void main(String args[]) throws MalformedURLException {
        WhelkManager manager = new WhelkManager(new URL("file:///tmp/out.txt"));
        manager.registerFactory("test", new TestFactory());
        Whelk whelk = manager.createWhelk("test", "test");
        
        if (whelk instanceof Pluggable) {
            ((Pluggable)whelk).addPlugin(new TestComponent());
            ((Pluggable)whelk).addPlugin(new TestComponent());
            ((Pluggable)whelk).addPlugin(new TestComponent());
            ((Pluggable)whelk).addPlugin(new TestComponent());
            ((Pluggable)whelk).addPlugin(new TestComponent());
        }
        
        System.out.println(manager.serialise());
        
        manager.save();
    }
}
