package org.infinispan.hadoopintegration.mapreduce.output;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface InfinispanOutputConverter<HadoopK, HadoopV, IspnK, IspnV> {

    IspnK convertKey(HadoopK key);

    IspnV convertValue(HadoopV value);
}
