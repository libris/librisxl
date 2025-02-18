package whelk.importer;

import whelk.Document;
import whelk.history.History;
import whelk.history.Ownership;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    public Merge(Map rulesMap) throws IOException {
        m_pathAddRules = new HashMap<>();
        m_pathReplaceRules = new HashMap<>();

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

    public void merge(Document base, Document incoming, String incomingAgent, History baseHistory) {
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

    public boolean mayAddAtPath(List<Object> path, List<Object> truePath, String incomingAgent, History history) {
        //System.err.println("** Testing if I may add at: " + path + " / " + truePath);
        //System.err.println("  testing against rules: " + m_pathAddRules);
        List<Object> temp = new ArrayList<>(path);
        List<Object> trueTemp = new ArrayList<>(truePath);

        Ownership owner = null;
        if (history != null) {
            owner = history.getOwnership(truePath);
        }

        while (!temp.isEmpty() && !trueTemp.isEmpty()) {
            if (m_pathAddRules.containsKey(temp)) {
                //System.err.println("  found rule! :" + temp + " matching true path: " + trueTemp + " existing owner is: " + owner);

                if (history == null) {
                    // There's a rule saying were allowed to add here, and we've been told to ignore all history (it was set to null), so we're good.
                    return true;
                }

                Map prioMap = m_pathAddRules.get(temp);
                if (prioMap == null) // No priority list given for this rule = anyone may add (unless hand-edited)!
                    return owner.m_manualEditor == null;

                int manualPrio = 0;
                int systematicPrio = 0;
                int incomingPrio = 0;
                if (owner.m_manualEditor != null && prioMap.get(owner.m_manualEditor) != null)
                    manualPrio = (Integer) prioMap.get(owner.m_manualEditor);
                if (owner.m_systematicEditor != null && prioMap.get(owner.m_systematicEditor) != null)
                    systematicPrio = (Integer) prioMap.get(owner.m_systematicEditor);
                if (prioMap.get(incomingAgent) != null)
                    incomingPrio = (Integer) prioMap.get(incomingAgent);

                if (manualPrio > 0) {
                    return false;
                }
                else if (incomingPrio >= systematicPrio) {
                    return true;
                }
                return false;
            }
            temp.remove(temp.size()-1);
            trueTemp.remove(trueTemp.size()-1);
        }
        return false;
    }

    private void mergeInternal(Object base, Object baseParent, Object correspondingIncoming,
                               List<Object> path,
                               List<Object> truePath, // Same as path, but without magic indexes, ..,0,mainTitle.. instead of ..,@type=Title,mainTitle.. for the BASE record, not incoming.
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
            if (baseHistory != null) {
                Set<Ownership> baseOwners = baseHistory.getSubtreeOwnerships(truePath);
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
                List<Object> trueChildPath = new ArrayList(truePath);
                trueChildPath.add(key);
                if ( ((Map) base).get(key) == null && ((Map) base).get(key + "ByLang") == null ) {
                    if (mayAddAtPath(childPath, trueChildPath, incomingAgent, baseHistory)) {
                        ((Map) base).put(key, ((Map) correspondingIncoming).get(key));
                        logger.info("Merge of " + loggingForID + ": added object at " + childPath);
                    }
                }

                // Keep scanning further down the tree!
                else {
                    mergeInternal( ((Map) base).get(key), base,
                            ((Map) correspondingIncoming).get(key),
                            childPath,
                            trueChildPath,
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

            // For each type in the _incoming_ list that does not exist in the base list,
            // consider add-if-none.
            Set<String> singleInstanceTypesInIncoming = findSingleInstanceTypesInOnlyA(incomingList, baseList);
            for (String type : singleInstanceTypesInIncoming) {
                List<Object> childPath = new ArrayList(path);
                childPath.add("@type="+type);
                List<Object> trueChildPath = new ArrayList(truePath);
                trueChildPath.add(indexOfType(incomingList, type));
                if (mayAddAtPath(childPath, trueChildPath, incomingAgent, baseHistory)) {
                    for (Object o : incomingList) {
                        Map m = (Map) o;
                        if (m.containsKey("@type") && m.get("@type") == type) {
                            baseList.add(m);
                        }
                    }
                    logger.info("Merge of " + loggingForID + ": added object at " + childPath);
                }
            }


            Set<String> singleInstanceTypesInBoth = findSingleInstanceTypesInBoth(baseList, incomingList);

            // For each type of which there is exactly one instance in each list
            for (String type : singleInstanceTypesInBoth) {

                // Find the one instance of that type in each list
                Map baseChild = null;
                Map incomingChild = null;
                int baseListInteger = -1;
                for (int i = 0; i < baseList.size(); ++i) {
                    Object o = baseList.get(i);
                    if (o instanceof Map) {
                        Map m = (Map) o;
                        if (m.containsKey("@type") && m.get("@type").equals(type)) {
                            baseListInteger = i;
                            baseChild = m;
                        }
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
                List<Object> trueChildPath = new ArrayList(truePath);
                trueChildPath.add(baseListInteger);
                mergeInternal( baseChild, base,
                        incomingChild,
                        childPath,
                        trueChildPath,
                        incomingAgent,
                        baseHistory,
                        loggingForID);
            }
        }
    }

    /**
     * Find the types of which there are exactly one instance in A and zero instances in B
     */
    private Set<String> findSingleInstanceTypesInOnlyA(List a, List b) {
        HashMap<String, Integer> typeCountsA = countTypes(a);
        HashMap<String, Integer> typeCountsB = countTypes(b);
        Set<String> singleInstanceTypes = new HashSet<>();
        for (String type : typeCountsA.keySet()) {
            if (typeCountsA.containsKey(type) &&
                    typeCountsA.get(type) == 1 &&
                    !typeCountsB.containsKey(type)) {
                singleInstanceTypes.add(type);
            }
        }
        return singleInstanceTypes;
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

    /**
     * Find the integer index of the _only_ instance having type = 'type'.
     */
    private int indexOfType(List list, String type) {
        for (int i = 0; i < list.size(); ++i) {
            if (list.get(i) instanceof Map) {
                Map m = (Map) list.get(i);
                if (m.get("@type") != null && m.get("@type").equals(type))
                    return i;
            }
        }
        return -1;
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
