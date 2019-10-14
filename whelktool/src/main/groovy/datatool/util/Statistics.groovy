package datatool.util

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class Statistics {
    ConcurrentHashMap<String, ConcurrentHashMap<Object, AtomicInteger>> c = new ConcurrentHashMap<>()

    void increment(String category, Object name) {
        c.computeIfAbsent(category, { new ConcurrentHashMap<>() })
        c.get(category).computeIfAbsent(name) { new AtomicInteger() }
        c.get(category).get(name).incrementAndGet()
    }

    void print() {
        println("STATISTICS")
        println("========================")
        for (Map.Entry e : c.entrySet().sort { a, b -> a.getKey().toString() <=> b.getKey().toString() }) {
            println(e.getKey())
            println("------------------------")
            List<Map.Entry<String, AtomicInteger>> entries = new ArrayList(c.get(e.getKey()).entrySet())
            entries.sort { a, b -> a.getKey().toString() <=> b.getKey().toString() }
            entries.sort { a, b -> b.getValue().intValue() <=> a.getValue().intValue() }
            entries.each {
                println("${it.getValue().intValue()} ${it.getKey()}")
            }
            println()
        }
    }

    Statistics printOnShutdown() {
        Runtime.getRuntime().addShutdownHook {
            this.print()
        }
        return this
    }

}