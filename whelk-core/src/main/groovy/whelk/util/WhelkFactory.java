package whelk.util;

import whelk.Whelk;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class WhelkFactory {
    private Whelk whelk;

    public WhelkFactory() {
        whelk = Whelk.createLoadedSearchWhelk();
    }

    public Whelk getWhelk() {
        return whelk;
    }

    public static Whelk getWhelkFromJndi() throws NamingException {
        Context initCtx = new InitialContext();
        Context envCtx = (Context) initCtx.lookup("java:comp/env");
        WhelkFactory factory = (WhelkFactory) envCtx.lookup("whelk/WhelkFactory");
        return factory.getWhelk();
    }
}
