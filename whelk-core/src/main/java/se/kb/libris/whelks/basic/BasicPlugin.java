package se.kb.libris.whelks.basic;

import se.kb.libris.whelks.plugin.Plugin;

public class BasicPlugin implements Plugin {
    private boolean enabled = true;
    private int order = 0;
    private String id = "basicPlugin";

    @Override
    public boolean isEnabled() { return enabled; }
    @Override
    public void enable() { this.enabled = true; }
    @Override
    public void disable() { this.enabled = false; }
    @Override
    public String getId() { return this.id; }
    @Override 
    public int getOrder() { return order; }
    public void setOrder(int o) { this.order = o; }
    @Override
    public void init(String whelkName) {}
    @Override
    public int compareTo(Plugin p) {
        int diff = this.getOrder() - p.getOrder();
        if (diff == 0) {
            // TreeSet seems to treat 0 compareTo-values as the same, thus not including them in the Set.
            diff++;
        }
        return diff;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!this.getClass().equals(obj.getClass())) {
            return false;
        }
        return this.getId().equals(((Plugin)obj).getId()) && this.getOrder() == ((Plugin)obj).getOrder();
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 17 + order;
        hash = hash * 31 + id.hashCode();
        hash = hash * 15 + (enabled ? 0 : 1);
        return hash;
    }
}
