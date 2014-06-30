package org.infinispan.hadoopintegration.mapreduce.input;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SegmentOwners implements Iterable<SocketAddress> {
    protected final SocketAddress[] addresses;

    public SegmentOwners(SocketAddress[] addresses) {
        this.addresses = addresses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SegmentOwners that = (SegmentOwners) o;

        return Arrays.equals(addresses, that.addresses);

    }

    @Override
    public int hashCode() {
        return addresses != null ? Arrays.hashCode(addresses) : 0;
    }

    @Override
    public Iterator<SocketAddress> iterator() {
        return new Iterator<SocketAddress>() {

            int i = 0;

            @Override
            public boolean hasNext() {
                return i < addresses.length;
            }

            @Override
            public SocketAddress next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return addresses[i++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public int size() {
        return addresses.length;
    }
}
