package com.vinodh.storm.topology.trident.filter;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class FilterTridentDemo extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return tridentTuple.getString(0).length() > 3;
    }
}
