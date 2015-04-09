package filter;

import filter.bloomfilter.RedisBloomFilter;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class URLFilter  extends BaseFilter {
    private RedisBloomFilter<String> bloomFilter;
    private final int EXPECTED_ELEMENT_COUNT = 100000;
    private final double DESIRED_FP = 0.01;
    private final String REDIS_HOST = "localhost";
    private final short REDIS_PORT = 6379;
    private final String BF_NAME = "Crawler_URL_BloomFilter";

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        bloomFilter = new RedisBloomFilter<String>(this.EXPECTED_ELEMENT_COUNT, this.DESIRED_FP,
                this.REDIS_HOST, this.REDIS_PORT, this.BF_NAME);
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        String url = tridentTuple.getString(0);

        if (bloomFilter.exists(url)) {
            System.out.println("----- BloomFilter reject (URL exists):" + url);
            return false;
        }
        bloomFilter.add(url);
        return true;
    }
}
