package org.infinispan.configuration.cache;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 08/12/12
 */
public class CustomStatsConfiguration {

    private boolean sampleServiceTimes = false;

    public boolean isSampleServiceTimes(){
        return sampleServiceTimes;
    }

    CustomStatsConfiguration(boolean sampleServiceTimes){
        this.sampleServiceTimes = sampleServiceTimes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomStatsConfiguration that = (CustomStatsConfiguration) o;

        return sampleServiceTimes == that.sampleServiceTimes;

    }

    @Override
    public int hashCode() {
        return (sampleServiceTimes ? 1 : 0);
    }

    @Override
    public String toString() {
        return "CustomStatsConfiguration{" +
                "sampleServiceTimes=" + sampleServiceTimes +
                '}';
    }
}
