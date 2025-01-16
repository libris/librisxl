package whelk.history;

import whelk.JsonLd;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static whelk.util.DocumentUtil.getAtPath;

public class History {

    /**
     * This class takes a list of versions of a record, and from that list compiles
     * two separate things.
     *
     * One is a map from 'path' to 'ownership', such that for example the path
     * {@graph,1,contribution} (and all subkeys!) are "owned" (most recently changed by)
     * for example "sigel S and was since additionally modified by a script".
     *
     * The other is a set change list, which contains a summary of what was updated
     * (and by whom) for each version of the record.
     */

    // Most-recent-ownership for each part of the record
    private final HashMap<List<Object>, Ownership> m_pathOwnership;

    // A (json) summary of changes or "change sets" for the history of this record, version for version.
    public Map m_changeSetsMap;

    // The last version added to this history, needed for diffing the next one against.
    private DocumentVersion m_lastVersion;

    private final JsonLd m_jsonLd;

    /**
     * Reconstruct a records history given a list of versions of said record
     */
    public History(List<DocumentVersion> versions, JsonLd jsonLd) {
        m_jsonLd = jsonLd;
        m_pathOwnership = new HashMap<>();

        m_changeSetsMap = new HashMap();
        m_changeSetsMap.put("@id", versions.get(0).doc.getCompleteId() + "/_changesets");
        m_changeSetsMap.put("changeSets", new ArrayList<>());

        // A list of all paths for which this added version claims ownership.
        // These are no longer set directly, because this list needs to be pruned
        // first. For example, we don't want a change in @graph,1,instanceOf,something
        // to cause a claim on all of @graph,1.
        List<List<Object>> claimedPaths = new ArrayList<>();

        // The list we get is sorted chronologically, oldest first.
        for (int i = 0; i < versions.size(); ++i) {
            DocumentVersion version = versions.get(i);

            Map changeSet = new HashMap();
            changeSet.put("@type", "ChangeSet");
            Map versionLink = new HashMap();
            versionLink.put("@id", version.doc.getCompleteId() + "/data?version=" + i);
            changeSet.put("version", versionLink);
            changeSet.put("addedPaths", new HashSet<>());
            changeSet.put("removedPaths", new HashSet<>());
            changeSet.put("agent", version.changedBy);
            List changeSets = (List) m_changeSetsMap.get("changeSets");
            changeSet.put("agent", getAgent(version));
            changeSet.put("date", version.doc.getModified());
            if (wasScriptEdit(version)) {
                changeSet.put("date", version.doc.getGenerationDate());
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/globalchanges");
                changeSet.put("tool", tool);
                HashMap agent = new HashMap();
                agent.put("@id", "https://libris.kb.se/library/SEK");
                changeSet.put("agent", agent);
            } else if ("APIX".equals(version.changedIn)) {
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/apix");
                changeSet.put("tool", tool);
            } else if ("batch import".equals(version.changedIn)) {
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/batchimport");
                changeSet.put("tool", tool);
            } else if ("vcopy".equals(version.changedIn)) {
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/voyager");
                changeSet.put("tool", tool);
            } else if ("WhelkCopier".equals(version.changedBy)) {
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/whelkcopier");
                changeSet.put("tool", tool);
            } else if ("xl".equals(version.changedIn)) { // Must be last in list!
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/crud");
                changeSet.put("tool", tool);
            }
            changeSets.add(changeSet);
            var previousVersion = m_lastVersion;
            addVersion(version, changeSet, claimedPaths);

            /*  Apply new ownerships. The rules for ownership are now:
                1. Consider only the composite path if/when available (so for example changing a subtitle
                   counts as having changed the title as a whole.
                2. Count only the deepest place of change. So while a change deep in a document could be considered
                   a change on many levels, only the actual place where data is different will be counted.
                3. If the modified thing was an element in a list: Claim only that element.
                4. If the modified thing was a property on some object, for example "issuanceType": Claim
                   the whole containing object (the instance in this case).
            */
            {
                for (List<Object> claimedPath : claimedPaths) {
                    boolean existsMoreSpecific = false;
                    for (List deeperPath : claimedPaths) {
                        if (isSubList(claimedPath, deeperPath) && deeperPath.size() > claimedPath.size()) {
                            existsMoreSpecific = true;
                        }
                    }

                    if (!existsMoreSpecific) {
                        List<Object> finalClaim;
                        if (claimedPath.get(claimedPath.size()-1) instanceof String) {
                            finalClaim = claimedPath.subList(0, claimedPath.size() - 1); // You own the next larger context than the one you changed.
                        } else
                            finalClaim = claimedPath;
                        m_pathOwnership.put(finalClaim, new Ownership(version, m_pathOwnership.get(finalClaim)));

                        //System.err.println("Claim:\n" + claimedPath + "\nreduced to:\n" + finalClaim);
                    }
                }
            }

            // Clean up markers that have a more specific equivalent
            {
                Set<List> added = (Set<List>) changeSet.get("addedPaths");
                Set<List> removed = (Set<List>) changeSet.get("removedPaths");
                
                Set<List> addedAndRemoved = new HashSet<>(added);
                addedAndRemoved.addAll(removed);
                
                for (List deeperPath : addedAndRemoved) {
                    added.removeIf(p -> !p.equals(deeperPath) && isSubList(p, deeperPath));
                    removed.removeIf(p -> !p.equals(deeperPath) && isSubList(p, deeperPath));
                }
                
                if (previousVersion != null) {
                    var prev = previousVersion.doc.data;
                    var curr = m_lastVersion.doc.data;
                    
                    for (var parent : added.stream().map(History::parent).collect(Collectors.toSet())) {
                        boolean isLangContainer = jsonLd.langContainerAliasInverted.containsKey(last(parent));
                        if (!isLangContainer && isAllChanged(getAtPath(prev, parent), getAtPath(curr, parent))) {
                            added.removeIf(p -> isSubList(parent, p));
                            added.add(parent);
                        }
                    }
                    
                    for (var parent : removed.stream().map(History::parent).collect(Collectors.toSet())) {
                        boolean isLangContainer = jsonLd.langContainerAliasInverted.containsKey(last(parent));
                        if (!isLangContainer && isAllChanged(getAtPath(prev, parent), getAtPath(curr, parent))) {
                            removed.removeIf(p -> isSubList(parent, p));
                            removed.add(parent);
                        }
                    }
                }
            }
        }
    }
        
    private static boolean isSubList(List a, List b) {
        if (a.size() > b.size())
            return false;
        if (b.subList(0, a.size()).equals(a))
            return true;
        return false;
    }
    
    private static boolean isAllChanged(Object a, Object b) {
        if (a == null || b == null) {
            return true;
        }
        if (a.getClass() != b.getClass()) {
            return true;
        }
        if (a.equals(b)) {
            return false;
        }
        if (a instanceof Map) {
            var ma = (Map<String, ?>) a;
            var mb = (Map<String, ?>) b;
            
            if (ma.isEmpty() || mb.isEmpty()) {
                return false;
            }
            
            if (JsonLd.isLink(ma) || JsonLd.isLink(mb)) {
                return true;
            }
            
            return intersect(ma.keySet(), mb.keySet()).size() == 0;
        }
        return false;
    }
    
    private static List parent(List path) {
        return path.subList(0, path.size() > 0 ? path.size() - 1 : 0);
    }
    
    private static <T> T last(List<T> l) {
        return l.size() > 0 ? l.get(l.size() - 1) : null;
    }  
    
    private static <T> Set<T> intersect(Set<T> a, Set<T> b) {
        var i = new HashSet<>(a);
        i.retainAll(b);
        return i;
    }

    public void addVersion(DocumentVersion version, Map changeSetToBuild, List<List<Object>> claimedPaths) {
        if (m_lastVersion == null) {
            m_pathOwnership.put( new ArrayList<>(), new Ownership(version, null) );
        } else {
            examineDiff(new ArrayList<>(), new ArrayList<>(), version, version.doc.data, m_lastVersion.doc.data, null, changeSetToBuild, claimedPaths);
        }
        m_lastVersion = version;
    }

    public Ownership getOwnership(List<Object> path) {
        List<Object> temp = new ArrayList<>(path);
        while (!temp.isEmpty()) {
            Ownership value = m_pathOwnership.get(temp);
            if (value != null) {
                //System.err.println("getOwnership of " + path + " exiting (0) with " + value.m_manualEditor + "/" + value.m_systematicEditor);
                return value;
            }
            temp.remove(temp.size()-1);
        }
        //System.err.println("getOwnership of " + path + " exiting (1) with default root owner");
        return m_pathOwnership.get(new ArrayList<>()); // The root (first) owner
    }

    /**
     * Get the set of owners for path and everything under it.
     */
    public Set<Ownership> getSubtreeOwnerships(List<Object> path) {
        Set<Ownership> owners = new HashSet<>();
        for (Object keyObject : m_pathOwnership.keySet()) {
            List<Object> key = (List<Object>) keyObject;
            if (key.size() >= path.size() && key.subList(0, path.size()).equals(path)) { // A path below (more specific) than 'path'
                owners.add(m_pathOwnership.get(key));
            }
        }

        // If there was no more specific ownership in the subtree, default
        // to the ownership of the base path (in other words, search upwards instead)
        if (owners.isEmpty()) {
            owners.add(getOwnership(path));
        }
        return owners;
    }

    /**
     * Examine differences between (what would presumably be) the same entity
     * in two versions of a record.
     *
     * 'version' is the new version (whole Record),
     * 'path' is where in the record(s) we are,
     * 'correspondingPath' is the "same" object in the old version
     * 'examining' is the object (entity?) being compared,
     * 'correspondingPrevious' is the "same" object in the old version
     * 'compositePath' is null or a (shorter/higher) path to the latest
     * found enclosing "composite object", such as for example a Title,
     * which is considered _one value_ even though it is structured and
     * has subcomponents. The point of this is that changing (for example)
     * a subTitle should result in ownership of the whole title (not just
     * the subtitle).
     * 'changeSet' is a map in which this function will be building a summary
     * of changes this version has.
     * 'claimedPaths' is a list which this function will be building of paths
     * for which the new version claims ownership
     */
    private void examineDiff(List<Object> path,
                             List<Object> correspondingPath,
                             DocumentVersion version,
                             Object examining, Object correspondingPrevious,
                             List<Object> compositePath,
                             Map changeSet,
                             List<List<Object>> claimedPaths) {
        if (examining instanceof Map) {

            if (! (correspondingPrevious instanceof Map) ) {
                claimPath(path, compositePath, claimedPaths);
                return;
            }

            Set k1 = ((Map) examining).keySet();
            Set k2 = ((Map) correspondingPrevious).keySet();

            // Is this a composite object ?
            Object type = ((Map)examining).get("@type");
            if ( type instanceof String &&
                    ( m_jsonLd.isSubClassOf( (String) type, "StructuredValue") ||
                            m_jsonLd.isSubClassOf( (String) type, "QualifiedRole") ) ) {
                compositePath = new ArrayList<>(path);
            }

            // Key added!
            if (!k2.containsAll(k1)) {
                Set newKeys = new HashSet(k1);
                newKeys.removeAll(k2);

                for (Object key : newKeys) {
                    List<Object> newPath = new ArrayList(path);
                    newPath.add(key);
                    claimPath(newPath, compositePath, claimedPaths);

                    ((HashSet) changeSet.get("addedPaths")).add(newPath);
                    //System.err.println(" Add: " + newPath);
                }
            }

            // Key removed!
            if (!k1.containsAll(k2)) {
                Set removedKeys = new HashSet(k2);
                removedKeys.removeAll(k1);

                for (Object key : removedKeys) {
                    List<Object> removedPath = new ArrayList(correspondingPath);
                    removedPath.add(key);
                    // The point of this is to set ownership of the _composite_ object if a part of it is removed.
                    claimPath(removedPath, compositePath, claimedPaths);
                    // The actual thing being removed however no longer exists and can be owned by no-one.
                    clearOwnership(removedPath);

                    ((HashSet) changeSet.get("removedPaths")).add(removedPath);
                    //System.err.println(" Rem: " + removedPath);
                }
            }
        }

        if (examining instanceof List) {
            if (! (correspondingPrevious instanceof List) ) {
                claimPath(path, compositePath, claimedPaths);
                ((HashSet) changeSet.get("addedPaths")).add(path);
                ((HashSet) changeSet.get("removedPaths")).add(correspondingPath);
                //System.err.println(" Add+Rem: " + path + " (which used to be) " + correspondingPath);
                return;
            }
        }

        if (examining instanceof String ||
                examining instanceof Float || examining instanceof Boolean) {
            if (!examining.equals(correspondingPrevious)) {
                claimPath(path, compositePath, claimedPaths);
                ((HashSet) changeSet.get("addedPaths")).add(path);
                ((HashSet) changeSet.get("removedPaths")).add(correspondingPath);
                //System.err.println(" Add+Rem: " + path + " (which used to be) " + correspondingPath + " due to diff of: " + examining + " and " + correspondingPrevious);
                return;
            }
        }

        // Keep scanning
        if (examining instanceof List) {
            // Removing from a list (reducing it in size) claims ownership of the list
            // Other removals mixed with additions cannot be distinguished from modifications
            if (((List) correspondingPrevious).size() > ((List) examining).size())
                claimPath(correspondingPath, null, claimedPaths);

            // Create copies of the two lists (so that they can be manipulated)
            // and remove from them any elements that _have an identical copy_ in the
            // other list.
            // This way, only elements that differ somehow remain to be checked, and
            // they remain in their relative order to one another.
            // Without this, removal or addition of a list element results in every
            // _following_ element being compared with the wrong element in the other list.
            List tempNew = new LinkedList((List) examining);
            List tempOld = new LinkedList((List) correspondingPrevious);
            for (int i = 0; i < tempNew.size(); ++i) {
                for (int j = 0; j < tempOld.size(); ++j) {
                    if (equalsDisregardOrder(tempNew.get(i), tempOld.get(j))) { // Will recursively check the entire subtree!
                        tempNew.remove(i);
                        tempOld.remove(j);
                        --i;
                        --j;
                        break;
                    }
                }
            }

            //System.err.println("Not equals (disregarding order) old:\n" + tempOld + "\nnew:\n" + tempNew);

            // What used to be at index x (in examining) is now at index y (in tempNew), and in analogue for tempOld
            Map<Integer, Integer> newToOldListIndices = new HashMap<>();
            Map<Integer, Integer> correspondingNewToOldListIndices = new HashMap<>();
            for (int i = 0; i < tempNew.size(); ++i) {
                newToOldListIndices.put(i, ((List) examining).indexOf( tempNew.get(i) ));
            }
            for (int i = 0; i < tempOld.size(); ++i) {
                correspondingNewToOldListIndices.put(i, ((List) correspondingPrevious).indexOf( tempOld.get(i) ));
            }

            // Find removed elements that are no longer there
            Iterator oldIt = tempOld.iterator();
            while (oldIt.hasNext()) {
                Object obj = oldIt.next();
                List list = (List) correspondingPrevious;
                for (int i = 0; i < list.size(); ++i) {

                    if (obj == list.get(i)) { // pointer identity is intentional
                        List<Object> newPath = new ArrayList<>(correspondingPath);
                        newPath.add(i);
                        ((HashSet) changeSet.get("removedPaths")).add(newPath);
                        //System.err.println(" Remove (and keep scanning): " + newPath);
                    }
                }
            }

            // Find new elements that weren't there before
            Iterator newIt = tempNew.iterator();
            while (newIt.hasNext()) {
                Object obj = newIt.next();
                List list = (List) examining;
                for (int i = 0; i < list.size(); ++i) {

                    if (obj == list.get(i)) { // pointer identity is intentional
                        List<Object> newPath = new ArrayList<>(path);
                        newPath.add(i);
                        ((HashSet) changeSet.get("addedPaths")).add(newPath);
                        claimPath(newPath, compositePath, claimedPaths);
                        //System.err.println(" Add (and keep scanning): " + newPath);
                    }
                }
            }

            for (int i = 0; i < tempNew.size(); ++i) {
                List<Object> childPath = new ArrayList(path);
                List<Object> correspondingChildPath = new ArrayList(correspondingPath);
                if ( tempOld.size() > i ) {
                    childPath.add(Integer.valueOf(newToOldListIndices.get(i)));
                    correspondingChildPath.add(Integer.valueOf(correspondingNewToOldListIndices.get(i)));
                    examineDiff(childPath, correspondingChildPath, version,
                            tempNew.get(i), ((List)correspondingPrevious).get(newToOldListIndices.get(i)),
                            compositePath, changeSet, claimedPaths);
                }
            }
        } else if (examining instanceof Map) {
            for (Object key : ((Map) examining).keySet() ) {
                List<Object> childPath = new ArrayList(path);
                List<Object> correspondingChildPath = new ArrayList(correspondingPath);
                if ( ((Map)correspondingPrevious).get(key) != null ) {
                    childPath.add(key);
                    correspondingChildPath.add(key);
                    examineDiff(childPath, correspondingChildPath, version,
                            ((Map) examining).get(key), ((Map) correspondingPrevious).get(key),
                            compositePath, changeSet, claimedPaths);
                }
            }
        }
    }

    private boolean equalsDisregardOrder(Object a, Object b) {
        if (a.getClass() != b.getClass()) {
            return false;
        }
        else if (a instanceof Map) {
            Map m1 = (Map) a;
            Map m2 = (Map) b;
            if (!m1.keySet().equals(m2.keySet()))
                return false;
            for (Object key : m1.keySet()) {
                if (!equalsDisregardOrder(m1.get(key), m2.get(key)))
                    return false;
            }
            return true;
        }
        else if (a instanceof List) {
            List l1 = (List) a;
            List l2 = (List) b;
            if (l1.size() != l2.size())
                return false;
            for (int i = 0; i < l1.size(); ++i) {
                for (int j = 0;; ++j) {
                    if (j == l2.size())
                        return false;
                    if (equalsDisregardOrder(l1.get(i), l2.get(j)))
                        break;
                }
            }
            return true;
        }
        return a.equals(b);
    }

    private void claimPath(List<Object> newPath, List<Object> compositePath, List<List<Object>> claimedPaths) {

        List<Object> path;
        if (compositePath != null) {
            path = compositePath;
        } else {
            path = newPath;
        }

        //System.err.println(" *** Claiming ownership of " + path);

        claimedPaths.add(path);
    }

    private void clearOwnership(List<Object> removedPath) {
        //System.err.println(" *** Clearing ownership of " + removedPath);
        Iterator<List<Object>> it = m_pathOwnership.keySet().iterator();
        while (it.hasNext()) {
            List<Object> keyPath = it.next();
            if (keyPath.size() >= removedPath.size() && keyPath.subList(0, removedPath.size()).equals(removedPath)) {
                // removedPath is a more general version of keyPath.
                // For example, keyPath might be @graph,1,hasTitle,subTitle
                // and the removed path @graph,1,hasTitle
                // Therefore, keyPath must be cleared.
                it.remove();
            }
        }
    }

    private static Map getAgent(DocumentVersion version) {
        Map agent = new HashMap();
        String uri = changedByToUri(version.changedBy);

        // Seems like there was a bug at some point where changedBy wasn't updated correctly by WhelkTool.
        // Consecutive versions have the same changedBy, generationProcess contains the correct value.
        if (uri.contains("sys/globalchanges") && !uri.equals(version.doc.getGenerationProcess())) {
            uri = version.doc.getGenerationProcess();
        }

        agent.put("@id", uri);
        return agent;
    }
    
    /**
     * What was put into the changedBy column has varied a bit over XLs history. This
     * tries to make sense of the different variants.
     */
    private static String changedByToUri(String changedBy) {
        if (changedBy == null)
            return "https://libris.kb.se/library/SEK";
        if (changedBy.startsWith("http"))
            return changedBy;
        if (changedBy.equals("")) // This was the case for script changes for a brief period (early global changes)
            return "https://libris.kb.se/library/SEK";
        if (changedBy.endsWith(".groovy"))
            return "https://libris.kb.se/library/SEK";
        if (changedBy.equals("Libriskörning, globala ändringar"))
            return "https://libris.kb.se/library/SEK";
        if (changedBy.equals("WhelkCopier"))
            return "https://libris.kb.se/library/SEK";
        else return "https://libris.kb.se/library/" + changedBy;
    }

    public static boolean wasScriptEdit(DocumentVersion version) {
        Instant modifiedInstant = ZonedDateTime.parse(version.doc.getModified()).toInstant();
        if (version.doc.getGenerationDate() != null) {
            Instant generatedInstant = ZonedDateTime.parse(version.doc.getGenerationDate()).toInstant();
            if (generatedInstant != null && generatedInstant.isAfter( modifiedInstant )) {
                return true;
            }
        }
        return false;
    }

    // DEBUG CODE BELOW THIS POINT
    public String toString() {
        StringBuilder b = new StringBuilder();
        toString(b, m_lastVersion.doc.data, 0, new ArrayList<>());
        return b.toString();
    }

    private void toString(StringBuilder b, Object current, int indent, List<Object> path) {
        if (current instanceof List) {
            for (int i = 0; i < ((List) current).size(); ++i) {
                beginLine(b, indent, path);
                b.append("[\n");
                List<Object> childPath = new ArrayList(path);
                childPath.add(Integer.valueOf(i));
                toString(b, ((List)current).get(i), indent + 1, childPath);
                b.setLength(b.length()-1); // drop newline
                b.append(",\n");
                beginLine(b, indent, path);
                b.append("]\n");
            }
        } else if (current instanceof Map) {
            beginLine(b, indent, path);
            b.append("{\n");
            for (Object key : ((Map) current).keySet() ) {
                List<Object> childPath = new ArrayList(path);
                childPath.add(key);
                beginLine(b, indent+1, childPath);
                b.append( "\"" + key + "\"" + " : \n");
                toString(b, ((Map) current).get(key), indent + 1, childPath);
                b.setLength(b.length()-1); // drop newline
                b.append(",\n");
            }
            beginLine(b, indent, path);
            b.append("}\n");
        } else {
            // Bool, string, number
            beginLine(b, indent, path);
            b.append("\"");
            b.append(current.toString());
            b.append("\"");
        }
    }

    private void beginLine(StringBuilder b, int indent, List<Object> path) {
        Formatter formatter = new Formatter(b);
        formatter.format("%1$-50s| ", getOwnership(path));
        for (int i = 0; i < indent; ++i) {
            b.append("  ");
        }
    }
}
