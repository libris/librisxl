package se.kb.libris.whelks.test;

import org.json.simple.JSONObject;
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.component.Component;
import se.kb.libris.whelks.persistance.JSONSerialisable;

public class TestComponent implements Component, JSONSerialisable {
    public void TestComponent() {
    }

    public String getId() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void enable() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void disable() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isEnabled() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setWhelk(Whelk w) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JSONObject serialize() {
        JSONObject _component = new JSONObject();
        
        _component.put("_classname", this.getClass().getName());
        _component.put("test", "test");
        
        return _component;
    }
}
