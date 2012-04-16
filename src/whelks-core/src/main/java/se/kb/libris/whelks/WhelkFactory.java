package se.kb.libris.whelks;

import java.lang.reflect.Constructor;
import java.util.Map;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.persistance.JSONInitialisable;
    
public abstract class WhelkFactory {
    public abstract Whelk create();
}
