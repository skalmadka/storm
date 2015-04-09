package function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


public class PrepareHrefKafka  extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String hrefList = tridentTuple.getString(0);
        Integer depth = Integer.parseInt(tridentTuple.getString(1));

        if(depth == 0)
            return;
        depth--;

        String[] hrefArray = hrefList.split(hrefList);
        for(String href : hrefArray) {
            tridentCollector.emit(new Values(href, depth));
        }
    }
}