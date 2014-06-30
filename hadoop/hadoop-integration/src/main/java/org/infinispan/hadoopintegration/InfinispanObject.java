package org.infinispan.hadoopintegration;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanObject<K> {

    K value;

    public InfinispanObject() {
        this(null);
    }

    public InfinispanObject(K value) {
        this.value = value;
    }

    public K get() {
        return value;
    }

    public void set(K value) {
        this.value = value;
    }
}
