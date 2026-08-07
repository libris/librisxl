package whelk.rest.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class JenaNamedGraphFilter extends Filter<ILoggingEvent> {

    private static final String SUPPRESSED_MESSAGE =
            "Only triples or default graph data expected : named graph data ignored";

    @Override
    public FilterReply decide(ILoggingEvent event) {
        String message = event.getFormattedMessage();
        if (message != null && message.contains(SUPPRESSED_MESSAGE)) {
            return FilterReply.DENY;
        }
        return FilterReply.NEUTRAL;
    }
}
