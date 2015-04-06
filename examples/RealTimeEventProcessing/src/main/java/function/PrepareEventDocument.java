package function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class PrepareEventDocument  extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String event = tridentTuple.getString(0);

        tridentCollector.emit(new Values("event_index", "event_type", Integer.toString(event.hashCode()), event ));
    }
}