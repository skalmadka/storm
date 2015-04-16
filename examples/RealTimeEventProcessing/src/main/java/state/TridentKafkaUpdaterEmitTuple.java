package state;

import java.util.List;
import storm.kafka.trident.TridentKafkaState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class TridentKafkaUpdaterEmitTuple extends BaseStateUpdater<TridentKafkaState> {
    public TridentKafkaUpdaterEmitTuple() {
    }

    public void updateState(TridentKafkaState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);

//        for(TridentTuple tuple : tuples)//Emit All Tuples
//            collector.emit(tuple);
    }
}