package trld;

import static java.util.AbstractMap.SimpleEntry;

import static trld.platform.Common.jsonEncode;

public class KeyValue<K, V> extends SimpleEntry<K, V> implements Comparable<KeyValue<K, V>> {
    public KeyValue(K k, V v) {
        super(k, v);
    }

    @Override
    public int compareTo(final KeyValue<K, V> other) {
        int compared = 0;
        if (getKey() instanceof Comparable) {
            compared = ((Comparable) getKey()).compareTo((Comparable) other.getKey());
        } else {
            compared = jsonEncode(getKey()).compareTo(jsonEncode(other.getKey()));
        }
        return compared == 0 ? jsonEncode(getValue()).compareTo(jsonEncode(other.getValue())) : compared;
    }
}
