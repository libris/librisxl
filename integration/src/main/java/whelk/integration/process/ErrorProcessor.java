package whelk.integration.process;

import org.apache.camel.Exchange;

public class ErrorProcessor implements org.apache.camel.Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        String error = exchange.getIn().getBody().toString();
    }
}
