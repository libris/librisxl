/**
 * Remove/replace broken SCB urls as per correspondence documented in https://jira.kb.se/browse/LXL-2537.
 *
 * We might want to reuse this script with more substitutions/removals in the future.
 *
 * Where we remove a broken url we also remove the entity containing that object.
 * Exceptions are thrown for deviant structures for which we can't guarantee that the entity is properly removed.
 * These are logged in 'exceptions.tsv' for manual handling.
 * The removed entities are logged in STATISTICS.txt (to verify that we don't remove anything valueable).
 *
 *
 */

import whelk.util.DocumentUtil
import java.util.regex.Pattern

PrintWriter replaced = getReportWriter('replaced.tsv')
PrintWriter removed = getReportWriter('removed.tsv')
PrintWriter unhandled = getReportWriter('unhandled.tsv')
// Might need manual handling:
PrintWriter exceptions = getReportWriter('exceptions.tsv')

Map substitutions =
        [
                "http://www.scb.se/statistik/_publikationer/": "http://share.scb.se/OV9993/Data/Publikationer/statistik/_publikationer/",
                "http://www.scb.se/statistik/"               : "http://share.scb.se/OV9993/Data/Publikationer/statistik/"
        ]

Pattern removePattern = ~/http:\/\/www\.scb\.se\/Pages\/List____[0-9]+\.aspx/

selectByCollection('bib') { data ->
    Map instance = data.graph[1]
    String id = data.doc.shortId

    List badPaths = []

    boolean modified = DocumentUtil.traverse(instance) { value, path ->
        if (value in String && value.startsWith('http://www.scb.se/')) {
            // Replace
            Map.Entry substitution = substitutions.find {value.startsWith(it.key) }
            if (substitution) {
                String newUrl = value.replace(substitution.key, substitution.value)
                replaced.println("$id\t$value\t$newUrl")
                return new DocumentUtil.Replace(newUrl)
            }

            // Remove
            if (value ==~ removePattern) {
                // Save path so that the object containing the broken link can be removed
                badPaths << path.collect()
                removed.println("${id}\t${path}\t${value}")
                return new DocumentUtil.Remove()
            }

            // Log unhandled broken links
            try {
                URLConnection conn = new URL(value).openConnection()

                if (conn.getResponseCode() in [301, 302])
                    conn = new URL(conn.getHeaderField('Location')).openConnection()

                int responseCode = conn.getResponseCode()

                if (responseCode in [400, 404]) {
                    unhandled.println("${id}\t${responseCode}\t${path}\t${value}")
                }
            } catch (Exception ex) {
                exceptions.println("${id}\t${ex}")
            }
        }
    }

    List containingObjectPaths = badPaths.findResults { bp ->
        try {
            return getContainingObjectPath(instance, bp)
        } catch (NewInPathException ex) {
            exceptions.println("${id}\t${ex}")
            return null
        }
    }

    // Remove the objects containing broken links altogether
    containingObjectPaths.reverseEach { p ->
        try {
            Object removedObj = removeContainingObject(instance, p)
            incrementStats("removed", removedObj)
            removeAllEmpty(instance, p)
        } catch (AlreadyRemovedException ex) {
            exceptions.println("${id}\t${ex}")
        } catch (NonEntityException ex) {
            exceptions.println("${id}\t${ex}")
        }
    }

    if (modified) {
        data.scheduleSave()
    }
}

List getContainingObjectPath(Object item, List path) {
    List containingPath = []

    for (p in path) {
        if (item[p] == null) {
            return containingPath
        } else {
            containingPath << p
            item = item[p]
        }
    }

    throw new NewInPathException("New object found at ${path}")
}

Object removeContainingObject(Object item, List path) {
    for (p in path[0..<-1]) {
        item = item[p]
        if (item == null)
            throw new AlreadyRemovedException("Object at ${path} already removed")
    }

    def last = path.last()

    if (!(item[last] in Map && item[last].containsKey('@type')))
        throw new NonEntityException("Object at ${path} doesn't seem to be an entity")

    return item.remove(last)
}

void removeAllEmpty(Object item, List path) {
    boolean removedEmpty = removeEmpty(item, path)
    while (removedEmpty) {
        removedEmpty = removeEmpty(item, path)
    }
}

boolean removeEmpty(Object item, List path) {
    for (p in path) {
        if (item[p] == null) {
            return false
        } else if (item[p].isEmpty()) {
            item.remove(p)
            return true
        } else {
            item = item[p]
        }
    }
}

class NewInPathException extends Exception {
    NewInPathException(String msg) { super(msg) }
}

class AlreadyRemovedException extends Exception {
    AlreadyRemovedException(String msg) { super(msg) }
}

class NonEntityException extends Exception {
    NonEntityException(String msg) { super(msg) }
}