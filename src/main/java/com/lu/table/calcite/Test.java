package com.lu.table.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin;

public class Test extends ConverterRule {
    public static final Test INSTANCE = new Test(Config.INSTANCE.withConversion(FlinkLogicalJoin.class, Convention.NONE,
            FlinkConventions.LOGICAL(), "Test"));

    protected Test(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        return null;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        JoinInfo joinInfo = join.analyzeCondition();
        return !joinInfo.pairs().isEmpty() || isSingleRowJoin(join);
    }

    private boolean isSingleRowJoin(LogicalJoin join) {
        if (join.getJoinType() == JoinRelType.INNER) {
            return isSingleRow(join.getRight()) || isSingleRow(join.getLeft());
        } else if (join.getJoinType() == JoinRelType.LEFT) {
            return isSingleRow(join.getRight());
        } else if (join.getJoinType() == JoinRelType.RIGHT) {
            return isSingleRow(join.getLeft());
        } else {
            return false;
        }
    }

    /**
     * Recursively checks if a [[RelNode]] returns at most a single row.
     * Input must be a global aggregation possibly followed by projections or filters.
     */
    private boolean isSingleRow(RelNode node) {
        if (node instanceof RelSubset) {
            return isSingleRow(((RelSubset) node).getOriginal());
        } else if (node instanceof Project) {
            return isSingleRow(((Project) node).getInput());
        } else if (node instanceof Filter) {
            return isSingleRow(((Filter) node).getInput());
        } else if (node instanceof Calc) {
            return isSingleRow(((Calc) node).getInput());
        } else if (node instanceof Aggregate) {
            return ((Aggregate) node).getGroupSets().isEmpty();
        } else {
            return false;
        }
    }
}
