package transform;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class ScriptGenerator
{
    public static void generate(Syntax oldSyntax, Syntax newSyntax, BufferedReader oldJsonReader, BufferedReader newJsonReader)
            throws IOException
    {
        // Generate syntax minus diff
        Set<Syntax.Rule> disappearingRules = new HashSet<>();
        disappearingRules.addAll( oldSyntax.rules );
        disappearingRules.removeAll( newSyntax.rules );

        // Generate syntax plus diff
        Set<Syntax.Rule> appearingRules = new HashSet<>();
        appearingRules.addAll( newSyntax.rules );
        appearingRules.removeAll( oldSyntax.rules );

        // For each (+) diff, attempt to find an equivalent (-) diff, matching on the values in the streams.
        // If one is found, the diff can be settled.

        ObjectMapper mapper = new ObjectMapper();
        String oldJsonString;
        while ( (oldJsonString = oldJsonReader.readLine()) != null)
        {
            Map oldData = mapper.readValue(oldJsonString, Map.class);
            Document doc = new Document(oldData);
            oldData = JsonLd.frame(doc.getCompleteId(), oldData);

            String newJsonString = newJsonReader.readLine();
            Map newData = mapper.readValue(newJsonString, Map.class);
            newData = JsonLd.frame(doc.getCompleteId(), newData);

            attemptToReduceDiff(appearingRules, disappearingRules, oldData, newData);
        }
    }

    private static void attemptToReduceDiff(Set<Syntax.Rule> appearingRules, Set<Syntax.Rule> disappearingRules,
                                     Map oldDataExample, Map newDataExample)
    {
        for (Syntax.Rule rule : appearingRules)
        {
            // Look for a value at appearingRule.path in the new document.
            // Try to find the same value in the old document, if so, at what path?

            String[] rulePathArray = (rule.path + "," + rule.followedByKey).split(",");
            LinkedList<String> rulePath = new LinkedList<>();
            for (String s: rulePathArray)
                rulePath.add(s);

            Object value = searchAtRulePath(rulePath , newDataExample);
            if (value == null)
                continue;

            String foundAtPath = searchForValue(oldDataExample, "_root", value);
            if (foundAtPath != null)
            {
                System.err.println("Tracked a move through value (" + value + "), "
                        + rule.path + "," + rule.followedByKey + " [has equivalent] " + foundAtPath);
            }
        }
    }

    private static Object searchAtRulePath(List<String> rulePath, Object data)
    {
        if (rulePath.size() == 0 || data == null)
            return data;

        String node = rulePath.get(0);
        rulePath.remove(0);
        if (node.equals("_root"))
        {
            node = rulePath.get(0);
            rulePath.remove(0);
        }

        if (node.equals("_list"))
        {
            List list = (List) data;
            for (int i = 0; i < list.size(); ++i)
            {
                Object element = ((List) data).get(i);
                Object result = searchAtRulePath(rulePath, element);
                if (result != null)
                    return result;
            }
        } else
        {
            data = ((Map) data).get(node);
            return searchAtRulePath(rulePath, data);
        }
        return null;
    }

    private static String searchForValue(Object data, String path, Object target)
    {
        if (data instanceof List)
        {
            List list = (List) data;
            for (int i = 0; i < list.size(); ++i)
            {
                Object element = list.get(i);
                String foundPath = searchForValue(element, path+","+i, target);
                if (foundPath != null)
                    return foundPath;
            }
        }

        if (data instanceof Map)
        {
            Map map = (Map) data;
            Set keySet = map.keySet();
            for (Object key : keySet)
            {
                String foundPath = searchForValue(map.get(key), path+","+key, target);
                if (foundPath != null)
                    return foundPath;
            }
        }

        if (data instanceof String) // Integer, Float, Boolean etc?
        {
            if (data.equals(target))
            {
                return path;
            }
        }

        return null;
    }
}
