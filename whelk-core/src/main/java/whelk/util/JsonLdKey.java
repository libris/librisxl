package whelk.util;

public interface JsonLdKey {
    String key();

    static <E extends Enum<E> & JsonLdKey> E fromKey(Class<E> enumClass, String key) {
        for (E e : enumClass.getEnumConstants()) {
            if (e.key().equals(key)) {
                return e;
            }
        }
        return null;
    }
}