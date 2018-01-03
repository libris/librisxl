package transform;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class SyntaxDiffReduce
{
    public static void generate(Syntax oldSyntax, Syntax newSyntax, BufferedReader oldJsonReader, BufferedReader newJsonReader)
            throws IOException
    {
        // Generate syntax minus diff
        Set<Syntax.Rule> disappearingRules = new HashSet<>();
        disappearingRules.addAll( oldSyntax.rules );
        disappearingRules.removeAll( newSyntax.rules );
        collapseRedundantRules(disappearingRules);

        // Generate syntax plus diff
        Set<Syntax.Rule> appearingRules = new HashSet<>();
        appearingRules.addAll( newSyntax.rules );
        appearingRules.removeAll( oldSyntax.rules );
        collapseRedundantRules(appearingRules);

        // A container for the resulting script
        Script script = new Script();

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

            attemptToReduceDiff(appearingRules, disappearingRules, oldData, newData, script);
            System.err.println("Still remaining +diff: " + appearingRules);
            System.err.println("Still remaining -diff: " + disappearingRules);
        }
    }

    private static void collapseRedundantRules(Set<Syntax.Rule> rules)
    {
        List<Syntax.Rule> rulesToRemove = new ArrayList<>();

        for (Syntax.Rule outerRule : rules)
        {
            String[] outerPath = outerRule.path.split(",");

            for (Syntax.Rule innerRule : rules)
            {
                // Is the inner rule implicitly covered by the outer?
                String[] innerPath = innerRule.path.split(",");
                String innerLeaf = innerRule.followedByKey;

                boolean innerIsSubRule = true;
                if (innerPath.length >= outerPath.length)
                {
                    innerIsSubRule = false;
                } else
                {
                    for (int i = 0; i < innerPath.length; ++i)
                    {
                        if ( ! innerPath[i].equals(outerPath[i]) )
                        {
                            innerIsSubRule = false;
                            break;
                        }
                    }

                    if (!innerLeaf.equals(outerPath[innerPath.length]))
                    {
                        innerIsSubRule = false;
                    }
                }

                if (innerIsSubRule)
                {
                    //System.err.println("Found that " + innerRule + " is contained within " + outerRule);
                    rulesToRemove.add(innerRule);
                }
            }
        }

        rules.removeAll(rulesToRemove);
    }

    private static void attemptToReduceDiff(Set<Syntax.Rule> appearingRules, Set<Syntax.Rule> disappearingRules,
                                     Map oldDataExample, Map newDataExample, Script script)
    {
        List<Syntax.Rule> rulesToRemoveFromDisappearing = new ArrayList<>();
        Iterator<Syntax.Rule> iterator = appearingRules.iterator();
        while (iterator.hasNext())
        {
            Syntax.Rule rule = iterator.next();

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

                script.resolveMove(foundAtPath, rule.path + "," + rule.followedByKey);
                // Remove this part of the diff (and if a corresponding "disappearing" rule exists)
                // remove that too
                iterator.remove();
                List<String> parts = Arrays.asList(foundAtPath.split(","));
                String correspondingPath = String.join(",", parts.subList(0, parts.size()-1));
                String followedByKey = parts.get(parts.size()-1);
                Syntax.Rule correspondingRule = new Syntax.Rule(correspondingPath, followedByKey);
                rulesToRemoveFromDisappearing.add(correspondingRule);
            }
        }
        disappearingRules.removeAll(rulesToRemoveFromDisappearing);
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
                String foundPath = searchForValue(element, path+",_list", target);
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
