package whelk.importer

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import whelk.Whelk

public class MockImporter extends Importer {
    public MockImporter(Whelk w)
    {
        whelk = w
    }
    void doImport(String collection)
    {
        // Do nothing, we wish only to trigger the version writing behavior of the abstract Importer class.
        // f-ing OOP BS, sigh.
    }

    @Override
    ImportResult doImport(String collection, String sourceSystem, Date from) {
        throw new NotImplementedException()
    }
}
