package com.lu.table.calcite;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin;

public class Test extends ConverterRule {

    protected Test() {
        super(FlinkLogicalJoin.class, FlinkConventions.LOGICAL(), FlinkConventions.DATASET(), "DataSetJoinRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        return null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

    }
}
