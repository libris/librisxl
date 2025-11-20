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
        EXPERIMENTAL_CATEGORY_COLLECTION,
        EXPERIMENTAL_INDEX_HOLDING_ORGS
    }

    private final Set<Flag> enabled = new HashSet<>();

    public FeatureFlags(Properties whelkConfig) {
        Arrays.stream(whelkConfig.getProperty("features", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(flag -> {
                    try {
                        enabled.add(Flag.valueOf(flag));
                        log.info("Enabled feature: {}", flag);
                    } catch (IllegalArgumentException ignored) {
                        log.warn("Unknown feature flag, ignoring: {}", flag);
                    }
                });
    }

    private FeatureFlags() {

    }

    public static FeatureFlags uninitialized() {
        return new FeatureFlags();
    }
    
    public boolean isEnabled(Flag flag) {
        return enabled.contains(flag);
    }
}
