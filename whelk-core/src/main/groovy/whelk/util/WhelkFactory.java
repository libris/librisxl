package whelk.util;

import whelk.Whelk;

public class WhelkFactory {
    private static Whelk whelk;

    public static synchronized Whelk getSingletonWhelk() {
        if (whelk == null) {
            whelk = Whelk.createLoadedSearchWhelk();
        }

        return whelk;
    }
}
