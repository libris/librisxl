package transform;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import whelk.util.TransformScript;
import java.util.Map;

public class ScriptTest
{
    static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testBasicSet() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set value0 -> key0";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    private void testScript(String beforeTransformText, String expectedResultText, String transformScript) throws Exception
    {
        Map oldData = mapper.readValue(beforeTransformText, Map.class);
        Map expectedResultData = mapper.readValue(expectedResultText, Map.class);

        TransformScript executableScript = new TransformScript(transformScript);
        Map transformed = executableScript.executeOn(oldData);

        if ( ! Utils.rdfEquals( expectedResultData, transformed ) )
        {
            System.out.println("expected transformation result:\n"+expectedResultData);
            System.out.println("\nactual transformation result:\n"+transformed);
            Assert.assertTrue( false );
        }
    }
}
