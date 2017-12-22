package transform;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class Syntax
{
    public class Rule
    {
        public Rule(String path, String followedByKey)
        {
            this.path = path;
            this.followedByKey = followedByKey;
        }

        public String toString()
        {
            return path + " -> " + followedByKey;
        }

        @Override
        public boolean equals(Object candidate)
        {
            if (candidate instanceof Rule)
            {
                Rule candidateRule = (Rule) candidate;
                return path.equals(candidateRule.path) && followedByKey.equals(candidateRule.followedByKey);
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return path.hashCode() + followedByKey.hashCode();
        }

        String path;
        String followedByKey;
    }

    public Set<Rule> rules = new HashSet<>();

    public Syntax() {}

    public Syntax(BufferedReader in) throws IOException
    {
        String line;
        while ( (line = in.readLine()) != null)
        {
            line = line.trim();
            if (line.equals(""))
                continue;

            String[] parts = line.split(" -> ");
            addRule(parts[0], parts[1]);
        }
    }

    public void expandSyntaxToCover(Map data)
    {
        parseDocument(data, "_root");
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (Rule rule : rules)
            sb.append(rule + "\n");
        return sb.toString();
    }

    private void addRule(String type, String followedByType)
    {
        rules.add(new Rule(type, followedByType));
    }

    private void parseDocument(Object data, String root)
    {
        if (data instanceof List)
        {
            List list = (List) data;
            for (Object element : list)
            {
                addRule(root, "_list");

                parseDocument(element, root+","+"_list");
            }
        }

        if (data instanceof Map)
        {
            Map map = (Map) data;
            Set keySet = map.keySet();
            for (Object key : keySet)
            {
                addRule(root, (String) key);

                parseDocument(map.get(key), root+","+key);
            }
        }
    }
}
