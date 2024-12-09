package whelk;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FeatureFlags {
    final static Logger log = LogManager.getLogger(FeatureFlags.class);
    
    public enum Flag {
        INDEX_BLANK_WORKS,
    }

    private Set<Flag> enabled = new HashSet<>();
    
    public FeatureFlags(Properties whelkConfig) {
        Arrays.stream(whelkConfig.getProperty("features", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(flag -> {
                    try {
                        enabled.add(Flag.valueOf(flag));
                        log.info(String.format("Enabled feature: %s", flag));
                    } catch (IllegalArgumentException ignored) {
                        log.warn(String.format("Unknown feature flag, ignoring: %s", flag));
                    }
                });
    }
    
    public boolean isEnabled(Flag flag) {
        return enabled.contains(flag);
    }
}
