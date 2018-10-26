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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testBasicFor() throws Exception
    {
        String data = "{\"list\":[2,3]}";

        String script = "mode normal " +
                "let index = 999 " +
                "for index : list " +
                "    set * list,index -> key0 " +
                "set index -> key1 ";

        String transformed = "" +
                "{" +
                "    \"list\":[2,3], " +
                "    \"key0\":2, " +
                "    \"key1\":999 " +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testEmbeddedSet() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set value0 -> key0,key1";

        String transformed = "" +
                "{" +
                "    \"key0\":{\"key1\" : \"value0\"}" +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testHightListIndexSet() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set value0 -> key0,1";

        String transformed = "" +
                "{" +
                "    \"key0\":[null,\"value0\"]" +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testBasicLet2() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "let x = 1";

        String transformed = "" +
                "{}";

        boolean dataShouldBeDirtied = false;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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
                "   \"result1\":3" +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testBasicWhile() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "let i = 0 " +
                "while (i < 4) " +
                "{ " +
                "    set i -> key0 " +
                "    let i = i + 1" +
                "} " +
                "while false set value1 -> \"NOPE\" ";

        String transformed = "" +
                "{" +
                "    \"key0\":3" +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, true);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
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

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testStringTrim() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set trim \" \n  abcde cde x     \" -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"abcde cde x\" " +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testCharEscape() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "if \"\\(\" == \"\\(\"" +
                "  set value0 -> key0 " +
                "if \"\\(\" == \"\\)\"" +
                "  set value1 -> key1 ";

        String transformed = "" +
                "{" +
                "    \"key0\":\"value0\" " +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testNegativeLiteralValues() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set -1 -> key0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":-1 " +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    @Test
    public void testSetOnListIndex() throws Exception
    {
        String data = "{}";

        String script = "mode normal " +
                "set value0 -> key0,0 ";

        String transformed = "" +
                "{" +
                "    \"key0\":[\"value0\"] " +
                "}";

        boolean dataShouldBeDirtied = true;
        testScript(data, transformed, script, dataShouldBeDirtied);
    }

    private void testScript(String beforeTransformText, String expectedResultText, String transformScript, boolean dataShouldBeDirtied) throws Exception
    {
        Map oldData = mapper.readValue(beforeTransformText, Map.class);
        Map expectedResultData = mapper.readValue(expectedResultText, Map.class);

        TransformScript executableScript = new TransformScript(transformScript);
        TransformScript.DataAlterationState alterationState = new TransformScript.DataAlterationState();
        Map transformed = executableScript.executeOn(oldData, alterationState);

        if ( ! Utils.rdfEquals( expectedResultData, transformed ) )
        {
            System.out.println("expected transformation result:\n"+expectedResultData);
            System.out.println("\nactual transformation result:\n"+transformed);
            Assert.assertTrue( false );
        }

        if (alterationState.getAltered() != dataShouldBeDirtied)
        {
            System.out.println("Alteration state after execution was unexpected:\n"+alterationState.getAltered());
            Assert.assertTrue( false );
        }
    }
}
