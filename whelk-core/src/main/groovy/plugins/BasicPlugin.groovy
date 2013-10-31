package se.kb.libris.whelks.plugin;

public class BasicPlugin implements Plugin {
    private boolean enabled = true;
    private String id = "basicPlugin";

    @Override
    public boolean isEnabled() { return enabled; }
    @Override
    public void setEnabled(boolean e) { this.enabled = e; }
    @Override
    public String getId() { return this.id; }
    public void setId(String i) { this.id = i; }
    @Override
    public void init(String whelkName) {}

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + id.hashCode();
        hash = hash * 15 + (enabled ? 0 : 1);
        return hash;
    }
}
