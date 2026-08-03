package whelk.housekeeping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZonedDateTime;

public abstract class HouseKeeper {

    private static final Logger log = LogManager.getLogger(HouseKeeper.class);

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
