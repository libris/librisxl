package whelk.importer;

import whelk.Document;
import whelk.Whelk;
import whelk.history.History;
import whelk.history.Ownership;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static whelk.util.Jackson.mapper;

public class Merge {
    /*
    Merge rules are placed in files and look something like this:
    {
        "rules": [
            {
                "operation": "replace",
                "path": ["@graph", 1, "hasTitle"],
                "priority":  {
                    "Utb1": 10,
                    "Utb2": 11
                }
            },
            {
                "operation": "add_if_none",
                "path": ["@graph", 1, "hasTitle"]
            }
        ]
    }

     */
    enum Operation {
        REPLACE,
        ADD_IF_NONE
    }

    class Rule {
        public Operation operation;
        public Map sigelPriority;
    }

    // Maps a path to a rule.
    private Map<List<Object>, Rule> m_pathRules = null;

    public Merge(File ruleFile) throws IOException {
        m_pathRules = new HashMap<>();
        
        Map rulesMap = mapper.readValue(ruleFile, Map.class);
        List rules = (List) rulesMap.get("rules");
        for (Object rule : rules) {
            Map ruleMap = (Map) rule;
            String op = (String) ruleMap.get("operation");
            List path = (List) ruleMap.get("path");
            Map prioMap = (Map) ruleMap.get("priority");

            Rule r = new Rule();
            r.sigelPriority = prioMap;
            if (op.equals("replace"))
                r.operation = Operation.REPLACE;
            else if (op.equals("add_if_none"))
                r.operation = Operation.ADD_IF_NONE;
            else
                throw new RuntimeException("Malformed import rule, no such operation: " + op);
            m_pathRules.put(path, r);
        }
    }

    public void merge(Document base, Document incoming, String incomingAgent, Whelk whelk) {
        History baseHistory = new History(whelk.getStorage().loadDocumentHistory(base.getShortId()), whelk.getJsonld());

        List<Object> baseGraphList = (List<Object>) base.data.get("@graph");
        List<Object> incomingGraphList = (List<Object>) incoming.data.get("@graph");
        for (int i = 0; i < Integer.min(baseGraphList.size(), incomingGraphList.size()); ++i) {
            List<Object> path = new ArrayList<>();
            path.add("@graph");
            path.add(i);
            mergeInternal(
                    baseGraphList.get(i),
                    incomingGraphList.get(i),
                    path,
                    incomingAgent,
                    baseHistory
            );
        }
    }

    /**
     * Get the rule (of specified operation) that applies at this path, if any exists.
     * this will be the most specific one of all rules, with that operation, that cover this path.
     */
    public Rule getRuleForPath(List<Object> path, Operation op) {
        List<Object> temp = new ArrayList<>(path);
        while (!temp.isEmpty()) {
            Rule value = m_pathRules.get(temp);
            if (value != null && value.operation == op)
                return value;
            temp.remove(temp.size()-1);
        }
        return null;
    }

    private void mergeInternal(Object base, Object correspondingIncoming,
                                      List<Object> path,
                                      String incomingAgent, History baseHistory) {
        Rule replaceRule = getRuleForPath(path, Operation.REPLACE);
        if (replaceRule != null) {

            // Determine priority for the incoming version
            int incomingPriorityHere = 0;
            if (replaceRule.sigelPriority.get(incomingAgent) != null) {
                incomingPriorityHere = (Integer) replaceRule.sigelPriority.get(incomingAgent);
            }

            // Determine (the maximum) priority for any part of the already existing subtree (below 'path')
            int basePriorityHere = 0;
            boolean baseContainsHandEdits = false;
            Set<Ownership> baseOwners = baseHistory.getSubtreeOwnerships(path);
            for (Ownership baseOwnership : baseOwners) {
                if (baseOwnership.m_manualEditTime != null)
                    baseContainsHandEdits = true;
                String baseAgent = baseOwnership.m_systematicEditor;
                if (replaceRule.sigelPriority.get(baseAgent) != null) {
                    int priority = (Integer) replaceRule.sigelPriority.get(baseAgent);
                    if (priority > basePriorityHere)
                    basePriorityHere = priority;
                }
            }

            // Execute replacement if appropriate
            if (!baseContainsHandEdits && incomingPriorityHere > basePriorityHere) {
                if (base instanceof Map) {
                    Map map = ( (Map) base );
                    if (!subtreeContainsLinks(map)) {
                        map.clear();
                        map.putAll( (Map) correspondingIncoming );
                    }
                }
                else if (base instanceof List) {
                    List list = ( (List) base );
                    list.clear();
                    list.addAll( (List) correspondingIncoming );
                }

                return; // scan no further (we've just replaced everything below us)
            }
        }

        if (base instanceof Map && correspondingIncoming instanceof Map) {
            for (Object key : ((Map) correspondingIncoming).keySet() ) {

                // Does the incoming record have properties here that we don't have and are allowed to add?
                List<Object> childPath = new ArrayList(path);
                childPath.add(key);
                if ( ((Map) base).get(key) == null ) {
                    Rule addRule = getRuleForPath(childPath, Operation.ADD_IF_NONE);
                    if (addRule != null) {
                        ((Map) base).put(key, ((Map) correspondingIncoming).get(key));
                    }
                }

                // Keep scanning further down the tree!
                else {
                    mergeInternal( ((Map) base).get(key),
                            ((Map) correspondingIncoming).get(key),
                            childPath,
                            incomingAgent,
                            baseHistory);
                }
            }
        }
    }

    private boolean subtreeContainsLinks(Object object) {
        if (object instanceof List) {
            for (Object element : (List) object) {
                if (subtreeContainsLinks(element))
                    return true;
            }
        } else if (object instanceof Map) {
            Map map = (Map) object;
            if (map.keySet().size() == 1 && map.get("@id") != null) {
                return true;
            } else {
                for (Object key : map.keySet()) {
                    if (subtreeContainsLinks(map.get(key))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
