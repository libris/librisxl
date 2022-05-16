package whelk.history;

import whelk.Document;
import whelk.JsonLd;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;

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
        m_changeSetsMap.put("@id", versions.get(0).doc.getCompleteId() + "/changesets");
        m_changeSetsMap.put("changeSets", new ArrayList<>());

        // The list we get is sorted chronologically, oldest first.
        for (int i = 0; i < versions.size(); ++i) {
            DocumentVersion version = versions.get(i);

            Map changeSet = new HashMap();
            changeSet.put("@type", "ChangeSet");
            Map versionLink = new HashMap();
            versionLink.put("@id", version.doc.getCompleteId() + "/data?version=" + i);
            changeSet.put("version", versionLink);
            changeSet.put("addedPaths", new ArrayList<>());
            changeSet.put("modifiedPaths", new ArrayList<>());
            changeSet.put("removedPaths", new ArrayList<>());
            changeSet.put("agent", version.changedBy);
            List changeSets = (List) m_changeSetsMap.get("changeSets");
            Map agent = new HashMap();
            agent.put("@id", changedByToUri(version.changedBy));
            changeSet.put("agent", agent);
            if (wasScriptEdit(version)) {
                changeSet.put("date", version.doc.getGenerationDate());
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/globalchanges");
                changeSet.put("tool", tool);
            } else if (version.changedIn.equals("APIX")) {
                changeSet.put("date", version.doc.getModified());
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/apix");
                changeSet.put("tool", tool);
            } else if (version.changedIn.equals("batch import")) {
                changeSet.put("date", version.doc.getModified());
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/batchimport");
                changeSet.put("tool", tool);
            } else if (version.changedIn.equals("vcopy")) {
                changeSet.put("date", version.doc.getModified());
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/voyager");
                changeSet.put("tool", tool);
            } else if (version.changedBy.equals("WhelkCopier")) {
                changeSet.put("date", version.doc.getModified());
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/whelkcopier");
                changeSet.put("tool", tool);
            } else if (version.changedIn.equals("xl")) { // Must be last in list!
                changeSet.put("date", version.doc.getModified());
                Map tool = new HashMap();
                tool.put("@id", "https://id.kb.se/generator/crud");
                changeSet.put("tool", tool);
            }
            changeSets.add(changeSet);

            addVersion(version, changeSet);

            // Clean up empty fields
            if ( ((List) changeSet.get("addedPaths")).isEmpty() )
                changeSet.remove("addedPaths");
            if ( ((List) changeSet.get("removedPaths")).isEmpty() )
                changeSet.remove("removedPaths");
            if ( ((List) changeSet.get("modifiedPaths")).isEmpty() )
                changeSet.remove("modifiedPaths");
        }
    }

    public void addVersion(DocumentVersion version, Map changeSetToBuild) {
        if (m_lastVersion == null) {
            m_pathOwnership.put( new ArrayList<>(), new Ownership(version, null) );
        } else {
            examineDiff(new ArrayList<>(), version, version.doc.data, m_lastVersion.doc.data, null, changeSetToBuild);
        }
        m_lastVersion = version;
    }

    public Ownership getOwnership(List<Object> path) {
        List<Object> temp = new ArrayList<>(path);
        while (!temp.isEmpty()) {
            Ownership value = m_pathOwnership.get(temp);
            if (value != null)
                return value;
            temp.remove(temp.size()-1);
        }
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
     * 'previousVersion' is the old one (whole Record),
     * 'path' is where in the record(s) we are,
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
     */
    private void examineDiff(List<Object> path,
                             DocumentVersion version,
                             Object examining, Object correspondingPrevious,
                             List<Object> compositePath,
                             Map changeSet) {
        if (examining instanceof Map) {

            if (! (correspondingPrevious instanceof Map) ) {
                setOwnership(path, compositePath, version);
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
                    setOwnership(newPath, compositePath, version);

                    ((List) changeSet.get("addedPaths")).add(newPath);
                }
            }

            // Key removed!
            if (!k1.containsAll(k2)) {
                Set removedKeys = new HashSet(k2);
                removedKeys.removeAll(k1);

                for (Object key : removedKeys) {
                    List<Object> removedPath = new ArrayList(path);
                    removedPath.add(key);
                    // The point of this is to set ownership of the _composite_ object if a part of it is removed.
                    setOwnership(removedPath, compositePath, version);
                    // The actual thing being removed however no longer exists and can be owned by no-one.
                    clearOwnership(removedPath);

                    ((List) changeSet.get("removedPaths")).add(removedPath);
                }
            }
        }

        if (examining instanceof List) {
            if (! (correspondingPrevious instanceof List) ) {
                setOwnership(path, compositePath, version);
                ((List) changeSet.get("modifiedPaths")).add(path);
                return;
            }
        }

        if (examining instanceof String ||
                examining instanceof Float || examining instanceof Boolean) {
            if (!examining.equals(correspondingPrevious)) {
                setOwnership(path, compositePath, version);
                ((List) changeSet.get("modifiedPaths")).add(path);
                return;
            }
        }

        // Keep scanning
        if (examining instanceof List) {
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
                    if (tempNew.get(i).equals(tempOld.get(j))) { // Equals will recursively check the entire subtree!
                        tempNew.remove(i);
                        tempOld.remove(j);
                        --i;
                        --j;
                        break;
                    }
                }
            }

            if (!tempNew.isEmpty() || !tempOld.isEmpty())
                ((List) changeSet.get("modifiedPaths")).add(path);

            for (int i = 0; i < tempNew.size(); ++i) {
                List<Object> childPath = new ArrayList(path);
                if ( tempOld.size() > i ) {
                    childPath.add(Integer.valueOf(i));
                    examineDiff(childPath, version,
                            tempNew.get(i), tempOld.get(i),
                            compositePath, changeSet);
                }
            }
        } else if (examining instanceof Map) {
            for (Object key : ((Map) examining).keySet() ) {
                List<Object> childPath = new ArrayList(path);
                if ( ((Map)correspondingPrevious).get(key) != null ) {
                    childPath.add(key);
                    examineDiff(childPath, version,
                            ((Map) examining).get(key), ((Map) correspondingPrevious).get(key),
                            compositePath, changeSet);
                }
            }
        }
    }

    private void setOwnership(List<Object> newPath, List<Object> compositePath,
                              DocumentVersion version) {
        List<Object> path;
        if (compositePath != null) {
            path = compositePath;
        } else {
            path = newPath;
        }
        m_pathOwnership.put( path, new Ownership(version, m_pathOwnership.get(path)) );
    }

    private void clearOwnership(List<Object> removedPath) {
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

    /**
     * What was put into the changedBy column has varied a bit over XLs history. This
     * tries to make sense of the different variants.
     */
    private String changedByToUri(String changedBy) {
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
