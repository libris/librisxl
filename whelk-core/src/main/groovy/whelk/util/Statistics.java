package whelk.util;

import com.google.common.base.Preconditions;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Statistics {
    ConcurrentHashMap<String, ConcurrentHashMap<Object, AtomicInteger>> c = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, ConcurrentHashMap<Object, ArrayBlockingQueue<Object>>> examples = new ConcurrentHashMap<>();

    ThreadLocal<Stack<Object>> context = ThreadLocal.withInitial(() -> null);

    int numExamples;

    public Statistics() {
        this(1);
    }

    public Statistics(int numExamples) {
        this.numExamples = numExamples;
    }

    public void increment(String category, Object name) {
        increment(category, name, null);
    }

    public void increment(String category, Object name, Object example) {
        Preconditions.checkNotNull(category);
        Preconditions.checkNotNull(name);
        c.computeIfAbsent(category, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();

        if (example == null) {
            example = contextExample();
        }
        if (example != null && numExamples > 0) {
            examples.computeIfAbsent(category, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(name, k -> new ArrayBlockingQueue<>(numExamples))
                    .offer(example);
        }
    }

    public void withContext(Object example, Runnable r) {
        Preconditions.checkNotNull(example);
        Preconditions.checkNotNull(r);
        try {
            if (context.get() == null) {
                context.set(new Stack<>());
            }
            context.get().push(example);
            r.run();
        }
        finally {
            context.get().pop();
        }
    }

    public Object contextExample() {
        if (context.get() != null && !context.get().isEmpty()) {
            return context.get().peek();
        }
        return null;
    }

    public void print() {
        print(0);
    }

    public void print(int min) {
        print(min, System.out);
    }

    public void print(int min, PrintStream out) {
        PrintWriter writer = new PrintWriter(out);
        print(min, writer);
        writer.flush();
    }

    public void print(int min, PrintWriter out) {
        out.println("STATISTICS");
        out.println("========================");
        List<String> categories = new ArrayList<>(c.keySet());
        categories.sort(Comparator.naturalOrder());
        for (String category : categories) {
            long total = c.get(category).values().stream().mapToLong(AtomicInteger::intValue).sum();
            String header = category + " (" + total + ")";
            out.println(header);
            out.println("-".repeat(header.length()));
            List<Map.Entry<Object, AtomicInteger>> entries = new ArrayList<>(c.get(category).entrySet());
            entries.sort(Comparator.comparing(e -> e.getKey().toString()));
            entries.sort(Comparator.comparing((Map.Entry<Object, AtomicInteger> e) -> e.getValue().intValue()).reversed());

            int digitWidth = Math.min(10, entries.stream().mapToInt(e -> String.valueOf(e.getValue().intValue()).length()).max().orElse(0));
            int nameWidth = Math.min(60, entries.stream().mapToInt(e -> e.getKey().toString().length()).max().orElse(0));
            String format = "%" + digitWidth + "s %-" + nameWidth + "s %s";
            for (Map.Entry<Object, AtomicInteger> entry : entries) {
                Object name = entry.getKey();
                int value = entry.getValue().intValue();
                if (value > min) {
                    ArrayBlockingQueue<Object> e = examples.containsKey(category) ? examples.get(category).get(name) : null;
                    out.println(String.format(format, value, name,
                            e != null ? new ArrayList<>(e).toString() : ""));
                }
            }
            out.println();
        }
    }

    public Statistics printOnShutdown() {
        return printOnShutdown(0);
    }

    public Statistics printOnShutdown(int min) {
        return printOnShutdown(min, System.out);
    }

    public Statistics printOnShutdown(int min, PrintStream out) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.print(min, out);
            out.flush();
        }));
        return this;
    }

    public boolean isEmpty() {
        return c.isEmpty();
    }
}
