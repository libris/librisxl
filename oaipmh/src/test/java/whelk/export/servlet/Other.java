package whelk.export.servlet;

import org.junit.Assert;
import org.junit.Test;
import whelk.Document;
import whelk.Whelk;
import whelk.triples.JsonldSerializer;

import java.util.List;
import java.util.Set;

public class Other {
    /**
     * These tests have nothing to do with OAIPMH. Rather they are here because this test suite is the only one that
     * combines requiring example data, and being able to do white box code testing (test code internals).
     */

    /**
     * This test case dissolves a record into triples, and then reassembles the tripples. The reassembled version
     * must equal the original version.
     */
    /* This testcase unfortunately breaks due to repeatableTerms/forcedSetTerms inconsistencies. Reactivate once fixed!
    @Test
    public void testTripleDissolutionReassembly() throws Exception
    {
        Whelk whelk = Whelk.createLoadedCoreWhelk();
        Set<String> repeatableTerms = whelk.getJsonld().getRepeatableTerms();

        Document originalDocument = whelk.getStorage().load("s93qhl340tcvtcp"); // Alice in Wonderland and Through the looking-glass

        List<String[]> triples = new JsonldSerializer().deserialize(originalDocument.data);

        Document reassembledDocument = new Document(JsonldSerializer.serialize(triples, repeatableTerms));
        JsonldSerializer.normalize(reassembledDocument.data, reassembledDocument.getCompleteId(), false);
        JsonldSerializer.normalize(reassembledDocument.data, reassembledDocument.getCompleteId(), false);

        //System.out.println("original:\n" + originalDocument.getDataAsString());
        //System.out.println("reassembly:\n" + reassembledDocument.getDataAsString());

        Assert.assertEquals(reassembledDocument.getChecksum(), originalDocument.getChecksum());
    }
    */
}
