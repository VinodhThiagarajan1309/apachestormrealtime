package com.vinodh.storm.topology.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SimpleTridentFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        tridentCollector.emit( new Values(tridentTuple.getString(0) + " After Processing"));
    }
}
