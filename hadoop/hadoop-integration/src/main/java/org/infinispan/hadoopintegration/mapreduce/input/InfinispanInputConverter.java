package org.infinispan.hadoopintegration.mapreduce.input;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface InfinispanInputConverter<HadoopK, HadoopV, IspnK, IspnV> {

    HadoopK createKey();

    HadoopV createValue();

    void setKey(HadoopK key, IspnK key2);

    void setValue(HadoopV value, IspnV value2);

}
