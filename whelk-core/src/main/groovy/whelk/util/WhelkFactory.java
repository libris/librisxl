package whelk.util;

import whelk.Whelk;

public class WhelkFactory {
    private static Whelk whelk;
    private static Whelk coreWhelk;

    public static synchronized Whelk getSingletonWhelk() {
        if (whelk == null) {
            whelk = Whelk.createLoadedSearchWhelk();
        }

        return whelk;
    }

    public static synchronized Whelk getSingletonCoreWhelk() {
        if (coreWhelk == null) {
            coreWhelk = Whelk.createLoadedCoreWhelk();
        }

        return coreWhelk;
    }
}
