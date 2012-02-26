package se.kb.libris.whelks.temp;

import java.io.Serializable;
import java.util.LinkedList;

public class GenericsTest {
    public Iterable<? extends Serializable> a() {
        return new LinkedList<String>();
    }
}
