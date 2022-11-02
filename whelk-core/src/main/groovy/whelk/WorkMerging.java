package whelk;

import whelk.component.PostgreSQLComponent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkMerging {

    /**
     * Merge the works of all listed instances into one. The listed instances
     * may or may not have external works already. Orphaned work records will be
     * deleted.
     *
     * This is _not_ one atomic operation, but rather a series of operations.
     * This means that it is possible to observe the process halfway though from the
     * outside. It also means that should the process be stopped halfway through,
     * results may look odd (but will still obey basic data integrity rules).
     *
     * In the worst case scenario, if the process is interrupted just after the orphans
     * have been deleted, but their sameAs-uris have not yet been moved to the merged
     * work, those sameAs-uris will be lost. This risk cannot be avoided without compromising
     * the URI integrity checks of the underlying code (two records are never allowed to
     * have the same URI at the same time).
     *
     * Returns the URI of the one remaining (or new) work that all of the instances
     * now link to.
     */
    public static String mergeWorksOf(List<String> instanceIDs, Whelk whelk) {

        List<Document> instances = collectInstancesOfThisWork(instanceIDs, whelk);

        Document baseWork = selectBaseWork(instances, whelk);
        String baseWorkUri = baseWork.getThingIdentifiers().get(0);

        // Relink the instances and collect all work aliases
        Map linkEntity = new HashMap();
        linkEntity.put("@id", baseWorkUri);
        List<String> workAlternateUris = new ArrayList<>();
        for (Document instance : instances) {
            workAlternateUris.addAll( instance.getThingIdentifiers() );
            if (!instance.getWorkEntity().equals(linkEntity)) { // If not already linked to the correct record
                whelk.storeAtomicUpdate(instance.getShortId(), true, false, true, "xl", null, (Document doc) -> {
                    doc.setWorkEntity(linkEntity);
                });
            }
        }

        // Merge other works into the baseWork. This must be done first, before any orphans can be deleted,
        // or we risk loosing data if the process is interrupted.
        whelk.storeAtomicUpdate(baseWork.getShortId(), true, false, true, "xl", null, (Document doc) -> {
            // TODO MERGE HERE
        });

        // Cleanup no longer linked work records
        for (Document instance : instances) {
            Map workEntity = instance.getWorkEntity();
            String workUri = (String) workEntity.get("@id");
            String workId = whelk.getStorage().getSystemIdByIri(workUri);
            if (workEntity.size() == 1
                    && workEntity.containsKey("@id")
                    && !workEntity.equals(linkEntity)
                    && whelk.getStorage().getDependers(workId).isEmpty()) {
                String orphanID = whelk.getStorage().getSystemIdByIri((String)workEntity.get("@id"));
                whelk.remove(orphanID, "xl", null);
            }
        }

        // We must now save the baseWork a second time, to add all of the sameAs identifiers.
        // These could not be added the first time, because they still belonged to other records
        // that were not yet deleted.
        whelk.storeAtomicUpdate(baseWork.getShortId(), true, false, true, "xl", null, (Document doc) -> {
            for (String uri : workAlternateUris)
                baseWork.addThingIdentifier(uri);
        });

        return baseWorkUri;
    }

    /**
     * Find the set of instances that should link to the merged work. This of course includes the
     * passed instanceIDs, but also any other instances already sharing a work with one of those IDs.
     */
    private static List<Document> collectInstancesOfThisWork(List<String> instanceIDs, Whelk whelk) {
        List<Document> instances = new ArrayList<>(instanceIDs.size());
        for (String instanceID : instanceIDs) {
            Document instance = whelk.getDocument(instanceID);
            instances.add( instance );

            // Are there other instances linking to the same work as 'instance' ? If so add them to the
            // collection to (possibly) re-link as well.
            Map workEntity = instance.getWorkEntity();
            if (workEntity.size() == 1 && workEntity.containsKey("@id")) {
                String workUri = (String) workEntity.get("@id");
                String workId = whelk.getStorage().getSystemIdByIri(workUri);
                for (String otherInstanceId : whelk.getStorage().getDependers(workId)) {
                    Document otherInstance = whelk.getDocument(otherInstanceId);
                    instances.add( otherInstance );
                }
            }
        }
        return instances;
    }

    /**
     * Select (or create+save) a work record that should be used going forward for
     * all of the passed instances.
     */
    private static Document selectBaseWork(List<Document> instances, Whelk whelk) {
        // Find all the works
        List<String> linkedWorkURIs = new ArrayList<>();
        List<Map> embeddedWorks = new ArrayList<>();
        for (Document instance : instances) {
            Map workEntity = instance.getWorkEntity();
            if (workEntity.size() == 1 && workEntity.containsKey("@id")) {
                linkedWorkURIs.add( (String) workEntity.get("@id"));
            } else {
                embeddedWorks.add(workEntity);
            }
        }

        // Pick a linked one if any such exist, otherwise break off an embedded one
        String baseWorkUri = null;
        if (!linkedWorkURIs.isEmpty()) {
            baseWorkUri = linkedWorkURIs.get(0); // TODO: Be a little smarter about _which_ work we pick?
        } else {
            Document newWork = new Document(embeddedWorks.get(0)); // TODO: Be a little smarter about _which_ work we break off?
            newWork.deepReplaceId(Document.getBASE_URI().toString() + IdGenerator.generate());
            newWork.setControlNumber(newWork.getShortId());
            whelk.createDocument(newWork, "xl", null, "auth", false);
            baseWorkUri = newWork.getThingIdentifiers().get(0);
        }

        return whelk.getStorage().loadDocumentByMainId(baseWorkUri);
    }
}
