package org.infinispan.configuration.cache;

import org.infinispan.configuration.Builder;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 08/12/12
 */
public class CustomStatsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<CustomStatsConfiguration> {

    private boolean sampleServiceTimes = true;

    CustomStatsConfigurationBuilder(ConfigurationBuilder builder) {
        super(builder);
    }

    public CustomStatsConfigurationBuilder sampleServiceTimes() {
        this.sampleServiceTimes = true;
        return this;
    }

    public CustomStatsConfigurationBuilder notSampleServiceTimes() {
        this.sampleServiceTimes = false;
        return this;
    }

    @Override
    public void validate() {
    }

    @Override
    public CustomStatsConfiguration create() {
        return new CustomStatsConfiguration(sampleServiceTimes);
    }

    @Override
    public CustomStatsConfigurationBuilder read(CustomStatsConfiguration template) {
        this.sampleServiceTimes = template.isSampleServiceTimes();
        return this;
    }

    @Override
    public String toString() {
        return "CustomStatsConfigurationBuilder{" +
                "sampleServiceTimes=" + sampleServiceTimes +
                '}';
    }
}