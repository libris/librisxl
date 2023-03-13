package whelk.util;

import java.lang.management.ManagementFactory;

public class ThreadDumper {

    public static String threadInfo(String nameFragment) {
        StringBuilder s = new StringBuilder();
        s.append(String.format("Dumping threads named [%s]", nameFragment)).append('\n');
        for (var t : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
            if (t.getThreadName().contains(nameFragment)) {
                s.append(t.getThreadName()).append('\n');
                
                s.append(t.getThreadState());
                if (t.getLockInfo() != null) {
                    s.append(" on ").append(t.getLockInfo());
                }
                s.append('\n');
                
                for (var ste : t.getStackTrace()) {
                    s.append("    at ").append(ste).append('\n');
                }
            }
            s.append('\n');
        }
        return s.toString();
    }
}
