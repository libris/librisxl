package whelk.importer;

import whelk.Document;
import whelk.Whelk;
import whelk.history.History;
import whelk.history.Ownership;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
                "path": ["@graph", 1, "hasTitle", "@type=Title", "subtitle"],
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
    The "paths" in use here work the same way PostgresSQL paths do, but with
    some exceptions.
    1. Other than the surrounding @graph list (@graph,X), it is not possible
       to specify list indexes. So for example:
       ["@graph", 1, "hasTitle", 0, "subtitle"] is not allowed (the "0" being the problem).
    2. It is however allowed to target elements in lists using type specifiers. So for example
       ["@graph", 1, "hasTitle", "@type=Title", "subtitle"] is ok, BUT WILL ONLY WORK if there
       is exactly one title with @type=Title in both existing and incoming records. If either
       record has more than one (or none) of these, there is no way to identify which one is
       being targeted.
     */

    // Contains paths where we're allowed to add things that don't already exist
    private Map<List<Object>, Map> m_pathAddRules = null;

    // Maps a path to a sigel priority list.
    private Map<List<Object>, Map> m_pathReplaceRules = null;

    private final Logger logger = LogManager.getLogger(this.getClass());

    public Merge(File ruleFile) throws IOException {
        m_pathAddRules = new HashMap<>();
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
                m_pathAddRules.put(path, prioMap);
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
                    baseHistory,
                    base.getShortId()
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

    public boolean mayAddAtPath(List<Object> path, String incomingAgent, History history) {
        List<Object> temp = new ArrayList<>(path);
        while (!temp.isEmpty()) {
            if (m_pathAddRules.containsKey(temp)) {

                Map prioMap = m_pathAddRules.get(temp);
                if (prioMap == null) // No priority list given for this rules = anyone may add
                    return true;

                Ownership owner = history.getOwnership(temp);
                int manualPrio = Integer.MIN_VALUE;
                int systematicPrio = Integer.MIN_VALUE;
                if (owner.m_manualEditor != null && prioMap.get(owner.m_manualEditor) != null)
                    manualPrio = (Integer) prioMap.get(owner.m_manualEditor);
                if (owner.m_systematicEditor != null && prioMap.get(owner.m_systematicEditor) != null)
                    systematicPrio = (Integer) prioMap.get(owner.m_systematicEditor);
                int incomingPrio = (Integer) prioMap.get(incomingAgent);

                if (incomingPrio > manualPrio && incomingPrio > systematicPrio) {
                    return true;
                }
                return false;
            }
            temp.remove(temp.size()-1);
        }
        return false;
    }

    private void mergeInternal(Object base, Object baseParent, Object correspondingIncoming,
                               List<Object> path,
                               String incomingAgent,
                               History baseHistory,
                               String loggingForID) {
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

            // Determine if the subtree that is to potentially be replaced contains links.
            // If it does, it should generally speaking not be replaced. But there is a list
            // of exceptions to this rule:
            boolean containsSanctifiedLinks = subtreeContainsLinks(base);
            for (int i = path.size(); i > 1; --i) {
                // instanceOf,language links may be overwritten
                if (i > 1 && path.get(i-1).equals("language") && path.get(i-2).equals("instanceOf")) {
                    containsSanctifiedLinks = false;
                }
                // publication,*,country links may be overwritten
                if (path.size() > 2 && path.get(i-1).equals("country") && path.get(i-3).equals("publication")) {
                    containsSanctifiedLinks = false;
                }
            }

            // Execute replacement if appropriate
            if (!baseContainsHandEdits && incomingPriorityHere >= basePriorityHere &&
                    !containsSanctifiedLinks) {
                if (baseParent instanceof Map) {
                    Map parentMap = (Map) baseParent;
                    parentMap.put(path.get(path.size()-1), correspondingIncoming);
                } else if (baseParent instanceof List) {
                    List parentList = (List) baseParent;
                    String typeToReplace = ((String) path.get(path.size()-1)).substring(6); // Strip away the @type=
                    for (int i = 0; i < parentList.size(); ++i) {
                        if (parentList.get(0) instanceof Map) {
                            Map m = (Map) parentList.get(0);
                            if (m.containsKey("@type") && m.get("@type").equals(typeToReplace)) {
                                parentList.set(i, correspondingIncoming);
                                break;
                            }
                        }
                    }
                }
                logger.info("Merge of " + loggingForID + ": replaced " + path + ". Max existing subtree priority was: " +
                        basePriorityHere + " and incoming priority was: " + incomingPriorityHere);

                return; // scan no further (we've just replaced everything below us)
            }
        }

        if (base instanceof Map && correspondingIncoming instanceof Map) {
            for (Object key : ((Map) correspondingIncoming).keySet() ) {

                // Does the incoming record have properties here that we don't have and are allowed to add?
                List<Object> childPath = new ArrayList(path);
                childPath.add(key);
                if ( ((Map) base).get(key) == null ) {
                    if (mayAddAtPath(childPath, incomingAgent, baseHistory)) {
                        ((Map) base).put(key, ((Map) correspondingIncoming).get(key));
                        logger.info("Merge of " + loggingForID + ": added object at " + childPath);
                    }
                }

                // Keep scanning further down the tree!
                else {
                    mergeInternal( ((Map) base).get(key), base,
                            ((Map) correspondingIncoming).get(key),
                            childPath,
                            incomingAgent,
                            baseHistory,
                            loggingForID);
                }
            }
        } else if (base instanceof List && correspondingIncoming instanceof List) {
            // The idea here, is that if a list contains only a single element (per type)
            // In both existing and incoming records, then we can allow ourselves to assume
            // that those elements represent the same entity. If however there are more than
            // one, then no such assumptions can be made.
            //
            // So for example given existing @graph,1,hasTitle :
            // [ { "@type":"Title", mainTitle:"A" }, { "@type":"SpineTitle", mainTitle:"B" } ]
            // and an incoming @graph,1,hasTitle :
            // [ { "@type":"Title", mainTitle:"C" }, { "@type":"SpineTitle", mainTitle:"D" } ]
            // We will allow the "path" @graph,1,hasTitle,@type=Title,mainTitle (with a type
            // specification instead of list index) to specify the one and only title entity with
            // that type.
            //
            // Only exact type matches are considered, inheritance is meaningless in this context!

            List baseList = (List) base;
            List incomingList = (List) correspondingIncoming;

            Set<String> singleInstanceTypes = findSingleInstanceTypesInBoth(baseList, incomingList);

            // For each type of which there is exactly one instance in each list
            for (String type : singleInstanceTypes) {

                // Find the one instance of that type in each list
                Map baseChild = null;
                Map incomingChild = null;
                for (Object o : baseList) {
                    if (o instanceof Map) {
                        Map m = (Map) o;
                        if (m.containsKey("@type") && m.get("@type").equals(type))
                            baseChild = m;
                    }
                }
                for (Object o : incomingList) {
                    if (o instanceof Map) {
                        Map m = (Map) o;
                        if (m.containsKey("@type") && m.get("@type").equals(type))
                            incomingChild = m;
                    }
                }

                // Keep scanning
                List<Object> childPath = new ArrayList(path);
                childPath.add("@type="+type);
                mergeInternal( baseChild, incomingList,
                        incomingChild,
                        childPath,
                        incomingAgent,
                        baseHistory,
                        loggingForID);
            }
        }
    }

    /**
     * Find the types of which there are exactly one instance in each list
     */
    private Set<String> findSingleInstanceTypesInBoth(List a, List b) {
        HashMap<String, Integer> typeCountsA = countTypes(a);
        HashMap<String, Integer> typeCountsB = countTypes(b);
        Set<String> singleInstanceTypes = new HashSet<>();
        for (String type : typeCountsA.keySet()) {
            if (typeCountsB.containsKey(type) &&
                    typeCountsA.get(type) == 1 &&
                    typeCountsB.get(type) == 1) {
                singleInstanceTypes.add(type);
            }
        }
        return singleInstanceTypes;
    }

    private HashMap<String, Integer> countTypes(List list) {
        HashMap<String, Integer> typeCounts = new HashMap<>();
        for (Object o : list) {
            if (o instanceof Map) {
                Map map = (Map) o;
                if (map.get("@type") != null) {
                    String type = (String) map.get("@type");
                    if (!typeCounts.containsKey(type)) {
                        typeCounts.put(type, 1);
                    } else {
                        typeCounts.put(type, typeCounts.get(type) + 1);
                    }
                }
            }
        }
        return typeCounts;
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
