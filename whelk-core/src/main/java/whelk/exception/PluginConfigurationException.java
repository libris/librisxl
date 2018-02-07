package whelk.exception;

public class PluginConfigurationException extends RuntimeException {

    public PluginConfigurationException(String msg) {
        super(msg);
    }

    public PluginConfigurationException(Throwable t) {
        super(t);
    }

    public PluginConfigurationException(String msg, Throwable t) {
        super(msg, t);
    }
}
