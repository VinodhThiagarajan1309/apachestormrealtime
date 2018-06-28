package com.vinodh.storm.topology.trident.map;

import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MapFunctionDemo implements MapFunction {


    @Override
    public Values execute(TridentTuple tridentTuple) {
       return new Values(tridentTuple.getString(0).toLowerCase());
    }
}
