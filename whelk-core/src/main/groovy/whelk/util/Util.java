package whelk.util;

import groovy.lang.Closure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Util {
    public static <T> Iterable<T> lazyIterableChain(List<Closure<Iterable<T>>> iterables) {
        return () -> new Iterator<T>() {
            List<Closure<Iterable<T>>> suppliers = new ArrayList(iterables);
            Iterator<T> current = null;

            @Override
            public boolean hasNext() {
                return iterator() != null && iterator().hasNext();
            }

            @Override
            public T next() {
                if(!hasNext()) {
                    throw new NoSuchElementException();
                }
                return iterator().next();
            }

            private Iterator<T> iterator() {
                if (current != null && current.hasNext()) {
                    return current;
                }

                if(!suppliers.isEmpty()) {
                    current = suppliers.remove(0).call().iterator();
                    return iterator();
                }

                return null;
            }
        };
    }
}
