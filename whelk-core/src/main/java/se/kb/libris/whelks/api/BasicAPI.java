package se.kb.libris.whelks.api;

public abstract class BasicAPI implements API {

    private boolean enabled = false;

    public abstract void request(APIRequestContext context);

    public boolean isEnabled() {
        return enabled;
    }

    public void enable() {
        enabled = true;
    }

    public void disable() {
        enabled = false;
    }
}

