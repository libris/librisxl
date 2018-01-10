package transform;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;
import whelk.util.TransformScript;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class SyntaxDiffReduce
{
    static final ObjectMapper mapper = new ObjectMapper();

    public static ScriptGenerator generateScript(Syntax oldSyntax, Syntax newSyntax, BufferedReader oldJsonReader, BufferedReader newJsonReader)
            throws IOException, TransformScript.TransformSyntaxException
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
        ScriptGenerator scriptGenerator = new ScriptGenerator();

        // For each (+) diff, attempt to find an equivalent (-) diff, matching on the values in the streams.
        // If one is found, the diff can be settled.

        String oldJsonString;
        while ( (oldJsonString = oldJsonReader.readLine()) != null)
        {
            Map oldData = mapper.readValue(oldJsonString, Map.class);
            Document doc = new Document(oldData);
            oldData = JsonLd.frame(doc.getCompleteId(), oldData);

            String newJsonString = newJsonReader.readLine();
            if (newJsonString == null)
                continue;
            Map newData = mapper.readValue(newJsonString, Map.class);
            newData = JsonLd.frame(doc.getCompleteId(), newData);

            //System.err.println("Start with +diff: " + appearingRules);
            //System.err.println("Start with -diff: " + disappearingRules);
            while(attemptToReduceDiff(appearingRules, disappearingRules, oldData, newData, scriptGenerator))
            {
                //System.err.println("Still remaining +diff: " + appearingRules);
                //System.err.println("Still remaining -diff: " + disappearingRules);
            }
        }

        for (Syntax.Rule rule : appearingRules)
        {
            scriptGenerator.m_warnings.add("# The following path contained data in the new format, but no equivalent could be found\n" +
                    "# in the old format. Did you map in new data from MARC? (Severity: LOW)\n" +
                    "# " + rule.path + "," + rule.followedByKey + "\n#");
        }

        for (Syntax.Rule rule : disappearingRules)
        {
            scriptGenerator.m_warnings.add("# The following path contained data in the old format, but no equivalent could be found\n" +
                    "# in the new format. Are we mapping in less data from MARC in the new format? (Severity: HIGH)\n" +
                    "# " + rule.path + "," + rule.followedByKey + "\n#");
        }

        return scriptGenerator;
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

    private static boolean attemptToReduceDiff(Set<Syntax.Rule> appearingRules, Set<Syntax.Rule> disappearingRules,
                                     Map oldDataExample, Map newDataExample, ScriptGenerator scriptGenerator)
            throws IOException, TransformScript.TransformSyntaxException
    {
        // First apply the current state of the script to the incoming data, so that any changes we make now
        // are not in conflict with any already added to the script.
        TransformScript executableScript = new TransformScript(scriptGenerator.toString());
        String transformed = executableScript.executeOn(mapper.writeValueAsString(oldDataExample));
        oldDataExample = mapper.readValue(transformed, Map.class);

        boolean hadEffect = false;

        List<Syntax.Rule> rulesToRemoveFromAppearing = new ArrayList<>();
        Iterator<Syntax.Rule> iterator = disappearingRules.iterator();
        while (iterator.hasNext())
        {
            Syntax.Rule rule = iterator.next();

            // Look for a value at disappearingRule.path in the old document.
            // Try to find the same value in the new document, if so, at what path?

            String[] rulePathArray = (rule.path + "," + rule.followedByKey).split(",");
            LinkedList<String> rulePath = new LinkedList<>();
            for (String s: rulePathArray)
                rulePath.add(s);

            Object value = searchAtRulePath(rulePath , oldDataExample);
            if (value == null)
                continue;

            String foundAtPath = searchForValue(newDataExample, "_root", value);
            if (foundAtPath != null)
            {
                hadEffect = true;
                String completeRulePath = rule.path + "," + rule.followedByKey;
                System.err.println("Tracked a move through value (" + value + "), "
                        + completeRulePath + " [has equivalent] " + foundAtPath);

                scriptGenerator.resolveMove(completeRulePath.substring(6, completeRulePath.length()),
                        foundAtPath.substring(6, foundAtPath.length()));
                // Remove this part of the diff (and if a corresponding "appearing" rule exists)
                // remove that too
                iterator.remove();
                List<String> parts = Arrays.asList(foundAtPath.split(","));
                String correspondingPath = String.join(",", parts.subList(0, parts.size()-1));
                String followedByKey = parts.get(parts.size()-1);
                Syntax.Rule correspondingRule = new Syntax.Rule(correspondingPath, followedByKey);
                rulesToRemoveFromAppearing.add(correspondingRule);
            }
            if (hadEffect)
                break;
        }
        appearingRules.removeAll(rulesToRemoveFromAppearing);

        return hadEffect;
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
            if (! (data instanceof List))
                return null;

            List list = (List) data;
            for (int i = 0; i < list.size(); ++i)
            {
                Object element = ((List) data).get(i);
                Object result = searchAtRulePath(new ArrayList<>(rulePath), element);
                if (result != null)
                    return result;
            }
        } else
        {
            if (! (data instanceof Map))
                return null;

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
