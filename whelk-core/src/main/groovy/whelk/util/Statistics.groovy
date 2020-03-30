package whelk.util

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class Statistics {
    ConcurrentHashMap<String, ConcurrentHashMap<Object, AtomicInteger>> c = new ConcurrentHashMap<>()

    void increment(String category, Object name) {
        c.computeIfAbsent(category, { new ConcurrentHashMap<>() })
        c.get(category).computeIfAbsent(name) { new AtomicInteger() }
        c.get(category).get(name).incrementAndGet()
    }

    void print(out = System.out) {
        out.println("STATISTICS")
        out.println("========================")
        for (Map.Entry e : c.entrySet().sort { a, b -> a.getKey().toString() <=> b.getKey().toString() }) {
            long total = c.get(e.getKey()).values().collect{it.intValue()}.sum()
            String header = "${e.getKey()} ($total)"
            out.println(header)
            out.println("-" * header.length())
            List<Map.Entry<String, AtomicInteger>> entries = new ArrayList(c.get(e.getKey()).entrySet())
            entries.sort { a, b -> a.getKey().toString() <=> b.getKey().toString() }
            entries.sort { a, b -> b.getValue().intValue() <=> a.getValue().intValue() }
            entries.each {
                out.println("${it.getValue().intValue()} ${it.getKey()}")
            }
            out.println()
        }
    }

    Statistics printOnShutdown(out = System.out) {
        Runtime.getRuntime().addShutdownHook {
            this.print(out)
            out.flush()
        }
        return this
    }

}