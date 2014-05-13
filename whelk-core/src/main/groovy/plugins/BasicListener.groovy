package se.kb.libris.whelks.plugin

class BasicListener extends BasicPlugin {

    private static BasicListener instance = null

    Map registry

    private BasicListener() {
    }

    public static BasicListener getInstance() {
        if (instance == null) {
            instance = new BasicListener()
        }
        return instance
    }
}
