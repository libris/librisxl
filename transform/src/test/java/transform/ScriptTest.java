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
    public void testComplexSet() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "let x = 3 " +
                "set ( 3 * x ) + 2 -> result0 " +
                "let y = hej " +
                "set y + baberiba -> result1 ";

        String transformed = "" +
                "{" +
                "   \"result0\":11," +
                "   \"result1\":\"hejbaberiba\"" +
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

    @Test
    public void testNonWhitespaceSeparatedOperators() throws Exception
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
                "let x=(sizeof*somelist*2) " +
                "set x->result0 " +
                "let y=sizeof sometext " +
                "set y->result1 " +
                "let z=sizeof*somekey " +
                "set z->result2 " +
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
    public void testBasicIf() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if true " +
                "  set value0 -> key0 " +
                "if false" +
                "  set notValue0 -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testStringComparison() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if hej == hej " +
                "  set value0 -> key0 " +
                "if hej == intehej " +
                "  set notValue0 -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testIntegerComparison() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if 456 == 455 + 1 " +
                "  set value0 -> key0 " +
                "if 1 == 2 " +
                "  set notValue0 -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testIntegerGreaterThan() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if 456 > 455 " +
                "  set value0 -> key0 " +
                "if 454 > 455 " +
                "  set value1 -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testIntegerLessThan() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if 454 < 455 " +
                "  set value0 -> key0 " +
                "if 456 < 455 " +
                "  set value1 -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testIntegerLessOrEqualThan() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if 454 <= 455 " +
                "  set value0 -> key0 " +
                "if 455 <= 455 " +
                "  set value1 -> key1 " +
                "if 456 <= 455 " +
                "  set value2 -> key2 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key1\":\"value1\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testIntegerGreaterOrEqualThan() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if 456 >= 455 " +
                "  set value0 -> key0 " +
                "if 455 >= 455 " +
                "  set value1 -> key1 " +
                "if 454 >= 455 " +
                "  set value2 -> key2 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key1\":\"value1\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testNotEquals() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if 454 != 455 " +
                "  set value0 -> key0 " +
                "if 455 != 455 " +
                "  set value1 -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testBooleanComparison() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if false == false " +
                "  set value0 -> key0 " +
                "if true == false " +
                "  set notValue0 -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testIfBlock() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if true { " +
                "  set value0 -> key0 " +
                "  set value1 -> key1" +
                "} " +
                "if true == false " +
                "  set notValue0 -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key1\":\"value1\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testIfBlockEnd() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if false { " +
                "  set value0 -> key0 " +
                "} " +
                "set value1 -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key1\":\"value1\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testLogicAnd() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if true && true { " +
                "  set value0 -> key0 " +
                "} " +
                "if true && false { " +
                "  set value1 -> key1 " +
                "} " +
                "if false && true { " +
                "  set value2 -> key2 " +
                "} " +
                "if false && false { " +
                "  set value3 -> key3 " +
                "} ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testLogicOr() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if true || true { " +
                "  set value0 -> key0 " +
                "} " +
                "if true || false { " +
                "  set value1 -> key1 " +
                "} " +
                "if false || true { " +
                "  set value2 -> key2 " +
                "} " +
                "if false || false { " +
                "  set value3 -> key3 " +
                "} ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key1\":\"value1\"," +
                "    \"key2\":\"value2\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testLogicNot() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if !false " +
                "  set value0 -> key0 " +
                "if !true" +
                "  set notValue0 -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testLogicComposite() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if !false && !false" +
                "  set value0 -> key0 " +
                "if (true && (false || true))" +
                "  set value1 -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key1\":\"value1\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testNullCheck() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if null == null" +
                "  set value0 -> key0 " +
                "if \"hej\" == null" +
                "  set value2 -> key2 " +
                "if \"hej\" != null" +
                "  set value3 -> key3 " +
                "if ( *nothing,at,path == null )" +
                "  set value1 -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\"," +
                "    \"key3\":\"value3\"," +
                "    \"key1\":\"value1\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testSubstring() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set substring abcde 1 3 -> key0";

        String transformed = "" +
                "{" +
                "    \"key0\":\"bc\"" +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testStringStartsWith() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set startswith abcde abc -> key0 " +
                "set startswith abcde ebc -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":true, " +
                "    \"key1\":false " +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testStringEndsWith() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set endswith abcde cde -> key0 " +
                "set endswith abcde ebc -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":true, " +
                "    \"key1\":false " +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testStringContains() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set contains abcde cde -> key0 " +
                "set contains abcde ebc -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":true, " +
                "    \"key1\":false " +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testStringIndexOf() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set indexof abcde cde -> key0 " +
                "set indexof abcde bc -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":2, " +
                "    \"key1\":1 " +
                "}";

        testScript(data, transformed, script);
    }

    @Test
    public void testStringReplace() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set replace abcde cde x -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"abx\" " +
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
