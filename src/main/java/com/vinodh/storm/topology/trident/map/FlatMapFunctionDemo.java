package com.vinodh.storm.topology.trident.map;

import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class FlatMapFunctionDemo implements FlatMapFunction {
    @Override
    public Iterable<Values> execute(TridentTuple tridentTuple) {
        List<Values> valuesList = new ArrayList<>();
        for(String word : tridentTuple.getString(0).split(" ")) {
            valuesList.add(new Values(word));
        }
        return valuesList;
    }
}
