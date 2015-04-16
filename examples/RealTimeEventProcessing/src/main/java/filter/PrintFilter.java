package filter;

import filter.bloomfilter.RedisBloomFilter;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;


public class PrintFilter  extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        System.out.println("PrintFilter: "+tridentTuple);
        return true;
    }
}

