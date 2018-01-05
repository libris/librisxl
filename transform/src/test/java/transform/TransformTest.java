package transform;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        TransformScript executableScript = new TransformScript(transformScript);
        String transformed = executableScript.executeOn(oldFormatExample);

        Map transformedData = mapper.readValue(transformed, Map.class);
        if ( ! rdfEquals( newData, transformedData ) )
        {
            System.out.println("expected transformation result:\n"+newFormatExample);
            System.out.println("\nactual transformation result:\n"+transformed);
            System.out.println("\ntransformed performed on:\n"+oldFormatExample);
            System.out.println("\nusing script:\n" + transformScript);

            Assert.assertTrue( false );
        }
    }

    private static boolean rdfEquals(Map m1, Map m2)
    {
        if (m1.size() != m2.size())
            return false;

        for (Object key : m1.keySet())
        {
            Object value = m1.get(key);

            if (!m2.containsKey(key))
                return false;

            if (value instanceof Map)
            {
                if ( ! rdfEquals( (Map) value, (Map) m2.get(key)) )
                    return false;
            }
            else if (value instanceof List)
            {
                if ( ! rdfEquals( (List) value, (List) m2.get(key)) )
                    return false;
            }
            else if ( ! value.equals(m2.get(key)) )
                return false;
        }

        return true;
    }

    private static boolean rdfEquals(List l1, List l2)
    {
        if (l1.size() != l2.size())
            return false;

        Set s1 = new HashSet(l1);
        Set s2 = new HashSet(l2);

        for (Object entry1 : s1)
        {
            boolean existsAMatch = false;
            for (Object entry2 : s2)
            {
                if (entry1 instanceof Map && entry2 instanceof Map)
                {
                    if ( rdfEquals( (Map) entry1, (Map) entry2) )
                        existsAMatch = true;
                }
                else if (entry1 instanceof List && entry2 instanceof List)
                {
                    if ( rdfEquals( (List) entry1, (List) entry2) )
                        existsAMatch = true;
                }
                else if (entry1 instanceof String && entry2 instanceof String)
                {
                    if ( entry1.equals(entry2) )
                        existsAMatch = true;
                }
            }
            if (!existsAMatch)
                return false;
        }

        return true;
    }
}
