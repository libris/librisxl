package se.kb.libris.whelks.test;

import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.WhelkFactory;

public class TestFactory extends WhelkFactory {
    @Override
    public Whelk create() {
        return new TestWhelk();
    }
}
