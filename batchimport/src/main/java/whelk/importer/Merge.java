package whelk.importer;

import whelk.Document;
import whelk.Whelk;
import whelk.history.History;
import whelk.history.Ownership;

import java.io.File;
import java.io.IOException;
import java.util.*;

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

    // Contains paths where we're allowed to add things that don't already exist
    private Set<List<Object>> m_pathAddRules = null;

    // Maps a path to a sigel priority list.
    private Map<List<Object>, Map> m_pathReplaceRules = null;

    public Merge(File ruleFile) throws IOException {
        m_pathAddRules = new HashSet<>();
        m_pathReplaceRules = new HashMap<>();
        
        Map rulesMap = mapper.readValue(ruleFile, Map.class);
        List rules = (List) rulesMap.get("rules");
        for (Object rule : rules) {
            Map ruleMap = (Map) rule;
            String op = (String) ruleMap.get("operation");
            List path = (List) ruleMap.get("path");
            Map prioMap = (Map) ruleMap.get("priority");

            if (op.equals("replace"))
                m_pathReplaceRules.put(path, prioMap);
            else if (op.equals("add_if_none"))
                m_pathAddRules.add(path);
            else
                throw new RuntimeException("Malformed import rule, no such operation: " + op);
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
                    baseGraphList,
                    incomingGraphList.get(i),
                    path,
                    incomingAgent,
                    baseHistory
            );
        }
    }

    /**
     * Get the sigel priority list for replacements that applies at this path, if any exists.
     * this will be the most specific one of all rules, with that operation, that cover this path.
     */
    public Map getReplaceRuleForPath(List<Object> path) {
        List<Object> temp = new ArrayList<>(path);
        while (!temp.isEmpty()) {
            Map prioMap = m_pathReplaceRules.get(temp);
            if ( prioMap != null )
                return prioMap;
            temp.remove(temp.size()-1);
        }
        return null;
    }

    public boolean existsAddRuleForPath(List<Object> path) {
        List<Object> temp = new ArrayList<>(path);
        while (!temp.isEmpty()) {
            if (m_pathAddRules.contains(path))
                return true;
            temp.remove(temp.size()-1);
        }
        return false;
    }

    private void mergeInternal(Object base, Object baseParent, Object correspondingIncoming,
                                      List<Object> path,
                                      String incomingAgent, History baseHistory) {
        Map replacePriority = getReplaceRuleForPath(path);
        if (replacePriority != null) {

            // Determine priority for the incoming version
            int incomingPriorityHere = 0;
            if (replacePriority.get(incomingAgent) != null) {
                incomingPriorityHere = (Integer) replacePriority.get(incomingAgent);
            }

            // Determine (the maximum) priority for any part of the already existing subtree (below 'path')
            int basePriorityHere = 0;
            boolean baseContainsHandEdits = false;
            Set<Ownership> baseOwners = baseHistory.getSubtreeOwnerships(path);
            for (Ownership baseOwnership : baseOwners) {
                if (baseOwnership.m_manualEditTime != null)
                    baseContainsHandEdits = true;
                String baseAgent = baseOwnership.m_systematicEditor;
                if (replacePriority.get(baseAgent) != null) {
                    int priority = (Integer) replacePriority.get(baseAgent);
                    if (priority > basePriorityHere)
                    basePriorityHere = priority;
                }
            }

            // Execute replacement if appropriate
            if (!baseContainsHandEdits && incomingPriorityHere >= basePriorityHere) {
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
                else { // String, number etc
                    if (baseParent instanceof Map) { // List is not relevant, as we can't navigate past them!
                        Map parentMap = (Map) baseParent;
                        parentMap.put(path.get(path.size()-1), correspondingIncoming);
                    }
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
                    if (existsAddRuleForPath(childPath)) {
                        ((Map) base).put(key, ((Map) correspondingIncoming).get(key));
                    }
                }

                // Keep scanning further down the tree!
                else {
                    mergeInternal( ((Map) base).get(key), base,
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
