package transform;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Map;

import whelk.util.TransformScript;

public class TransformTest
{
    static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testBasicMove() throws Exception
    {
        String oldFormatExample = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        String newFormatExample = "" +
                "{" +
                "    \"key1\":\"value0\"" +
                "}";

        testTransform(oldFormatExample, oldFormatExample, newFormatExample, newFormatExample);
    }

    @Test
    public void testListMove() throws Exception
    {
        String oldFormatExample = "" +
                "{" +
                "\"list\":" +
                "[" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}," +
                "{" +
                "    \"key1\":\"value1\"" +
                "}" +
                "]" +
                "}";

        String newFormatExample = "" +
                "{" +
                "\"someobject\":" +
                "{" +
                "\"list\":" +
                "[" +
                "{" +
                "\"key0\":\"value0\"" +
                "}," +
                "{" +
                "\"key1\":\"value1\"" +
                "}" +
                "]" +
                "}" +
                "}";

        testTransform(oldFormatExample, oldFormatExample, newFormatExample, newFormatExample);
    }

    @Test
    public void testReverseListMove() throws Exception
    {
        String newFormatExample = "" +
                "{" +
                "\"list\":" +
                "[" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}," +
                "{" +
                "    \"key1\":\"value1\"" +
                "}" +
                "]" +
                "}";

        String oldFormatExample = "" +
                "{" +
                "\"someobject\":" +
                "{" +
                "\"list\":" +
                "[" +
                "{" +
                "\"key0\":\"value0\"" +
                "}," +
                "{" +
                "\"key1\":\"value1\"" +
                "}" +
                "]" +
                "}" +
                "}";

        testTransform(oldFormatExample, oldFormatExample, newFormatExample, newFormatExample);
    }

    @Test
    public void testPreserveValue() throws Exception
    {
        String oldFormatExample = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        String newFormatExample = "" +
                "{" +
                "    \"key1\":\"value0\"" +
                "}";

        String toBeTransformed = "" +
                "{" +
                "    \"key0\":\"OTHERVALUE\"" +
                "}";

        String expectedResult = "" +
                "{" +
                "    \"key1\":\"OTHERVALUE\"" +
                "}";

        testTransform(oldFormatExample, toBeTransformed, newFormatExample, expectedResult);
    }

    @Test
    public void testExtraListInTarget() throws Exception
    {
        String oldFormatExample = "" +
                "{" +
                "    \"key0\":\"value0\"" +
                "}";

        String newFormatExample = "" +
                "{" +
                "    \"somelist\":[{\"key1\":\"value0\"}]" +
                "}";

        testTransform(oldFormatExample, oldFormatExample, newFormatExample, newFormatExample);
    }

    @Test
    public void testExtraListInSource() throws Exception
    {
        String oldFormatExample = "" +
                "{" +
                "    \"somelist\":[{\"key0\":\"value0\"}]" +
                "}";

        String newFormatExample = "" +
                "{" +
                "    \"key1\":\"value0\"" +
                "}";

        testTransform(oldFormatExample, oldFormatExample, newFormatExample, newFormatExample);
    }

    private void testTransform(String oldFormatExample, String toBeTransformed,
                               String newFormatExample, String expectedTransformedResult) throws Exception
    {
        Map oldData = mapper.readValue(oldFormatExample, Map.class);
        Map newData = mapper.readValue(newFormatExample, Map.class);

        Syntax oldSyntax = new Syntax();
        oldSyntax.expandSyntaxToCover(oldData);

        Syntax newSyntax = new Syntax();
        newSyntax.expandSyntaxToCover(newData);

        ScriptGenerator scriptGenerator = SyntaxDiffReduce.generateScript(oldSyntax, newSyntax,
                new BufferedReader(new StringReader(oldFormatExample)),
                new BufferedReader(new StringReader(newFormatExample)));

        String transformScript = scriptGenerator.toString();

        System.err.println("\nResulting script:\n" + transformScript);

        TransformScript executableScript = new TransformScript(transformScript);
        String transformed = executableScript.executeOn(toBeTransformed);

        Map transformedData = mapper.readValue(transformed, Map.class);
        Map expectedData = mapper.readValue(expectedTransformedResult, Map.class);
        if ( ! Utils.rdfEquals( expectedData, transformedData ) )
        {
            System.out.println("expected transformation result:\n"+expectedTransformedResult);
            System.out.println("\nactual transformation result:\n"+transformed);
            System.out.println("\ntransformed performed on:\n"+oldFormatExample);
            System.out.println("\nusing script:\n" + transformScript);

            Assert.assertTrue( false );
        }
    }
}
