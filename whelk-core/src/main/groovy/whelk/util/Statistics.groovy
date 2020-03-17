package whelk.util

import com.google.common.base.Preconditions

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class Statistics {
    ConcurrentHashMap<String, ConcurrentHashMap<Object, AtomicInteger>> c = new ConcurrentHashMap<>()
    ConcurrentHashMap<String, ConcurrentHashMap<Object, ArrayBlockingQueue<Object>>> examples = new ConcurrentHashMap<>()

    ThreadLocal<Stack<Object>> context = ThreadLocal.withInitial({ -> null })

    int numExamples
    
    Statistics(int numExamples = 1) {
        this.numExamples = numExamples
    }

    void increment(String category, Object name, Object example = null) {
        Preconditions.checkNotNull(category)
        Preconditions.checkNotNull(name)
        c.computeIfAbsent(category, { new ConcurrentHashMap<>() })
                .computeIfAbsent(name) { new AtomicInteger() }.incrementAndGet()
        
        example = example ?: contextExample()
        if (example != null && numExamples > 0) {
            examples.computeIfAbsent(category, { new ConcurrentHashMap<>() })
                    .computeIfAbsent(name) { new ArrayBlockingQueue<Object>(numExamples) }
                    .offer(example)
        }
    }

    void withContext(Object example, Closure c) {
        Preconditions.checkNotNull(example)
        Preconditions.checkNotNull(c)
        try {
            if (!context.get()) {
                context.set(new Stack<Object>())
            }
            context.get().push(example)
            c.run()
        }
        finally {
            context.get().pop()
        }
    }
    
    Object contextExample() {
        if(context.get() && !context.get().isEmpty()) {
            context.get().peek()
        }
    }

    void print(int min = 0, out = System.out) {
        out.println("STATISTICS")
        out.println("========================")
        for (Map.Entry e : c.entrySet().sort { a, b -> a.getKey().toString() <=> b.getKey().toString() }) {
            String category = e.getKey()
            long total = c.get(category).values().collect{it.intValue()}.sum()
            String header = "${category} ($total)"
            out.println(header)
            out.println("-" * header.length())
            List<Map.Entry<String, AtomicInteger>> entries = new ArrayList(c.get(category).entrySet())
            entries.sort { a, b -> a.getKey().toString() <=> b.getKey().toString() }
            entries.sort { a, b -> b.getValue().intValue() <=> a.getValue().intValue() }

            int digitWidth = Math.min(10, entries.collect{ "${it.getValue().intValue()}".size() }.max())
            int nameWidth = Math.min(60, entries.collect{ it.getKey().toString().size() }.max())
            String format = "%${digitWidth}s %-${nameWidth}s %s"
            entries.each {
                Object name = it.getKey()
                int value = it.getValue().intValue()
                if (value > min) {
                    out.println(String.format(format, value, name,
                            (examples.get(category)?.get(name)?.collect()?.toString()) ?: ''))
                }
            }
            out.println()
        }
    }

    Statistics printOnShutdown(int min = 0, out = System.out) {
        Runtime.getRuntime().addShutdownHook {
            this.print(min, out)
            out.flush()
        }
        return this
    }

    boolean isEmpty() {
        c.isEmpty()
    }
}