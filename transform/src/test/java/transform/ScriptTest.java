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

    @Test
    public void testBasicLet() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set value0 -> key0 " +
                "let x = 1";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testLetSetMove() throws Exception
    {
        String data = "{\"key0\":\"value0\"}";

        String script = "mode normal " +
                "let var = * key0 " +
                "set var -> key1 " +
                "set literal -> key2 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key1\":\"value0\"," +
                "    \"key2\":\"literal\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testBasicParenthesis() throws Exception
    {
        String data = "{\"key0\":\"value0\"}";

        String script = "mode normal " +
                "let var = ( * key0 ) " +
                "set ( var ) -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key1\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testBasicArithmetic() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "let x = 1 + 1 " +
                "set x -> key0 " +
                "let y = 1 + 1 + 2 " +
                "set y -> key1 " +
                "let z = ( 1 + 1 ) * 3 " +
                "set z -> key2 ";

        String transformed = "" +
                "{" +
                "    \"key0\":2," +
                "    \"key1\":4," +
                "    \"key2\":6" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testSizeOf() throws Exception
    {
        String data = "" +
                "{" +
                "   \"somelist\":" +
                "   [" +
                "       {\"somekey\":\"somevalue\"}," +
                "       {\"somekey\":\"somevalue\"}," +
                "       {\"somekey\":\"somevalue\"}" +
                "   ]," +
                "   \"somekey\":\"somevalue\"" +
                "}";

        String script = "mode normal " +
                "let x = sizeof * somelist * 2 " +
                "set x -> result0 " +
                "let y = sizeof sometext " +
                "set y -> result1 " +
                "let z = sizeof * somekey " +
                "set z -> result2 " +
                "delete somelist " +
                "delete somekey";

        String transformed = "" +
                "{" +
                "   \"result0\":6," + // list length, somelist
                "   \"result1\":8," + // string length "sometext"
                "   \"result2\":9" + // string length "somevalue"
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testLocalScope() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "let x = 2 " +
                "{ " +
                "   let x = 3 " +
                "   set x -> result0 " +
                "} " +
                "set x -> result1 ";

        String transformed = "" +
                "{" +
                "   \"result0\":3," +
                "   \"result1\":2" +
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
