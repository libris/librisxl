package whelk.housekeeping;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.ZonedDateTime;

public abstract class HouseKeeper {

    private static final Logger log = LoggerFactory.getLogger(HouseKeeper.class);

    public abstract String getName();

    public abstract String getStatusDescription();

    public abstract String getCronSchedule();

    public abstract void trigger();

    public ZonedDateTime lastFailAt = null;
    public ZonedDateTime lastRunAt = null;

    synchronized void _trigger() {
        try {
            trigger();
            lastRunAt = ZonedDateTime.now();
        } catch (Throwable e) {
            log.error("Could not handle throwable in Housekeeper TimerTask.", e);
            lastFailAt = ZonedDateTime.now();
        }
    }
}
