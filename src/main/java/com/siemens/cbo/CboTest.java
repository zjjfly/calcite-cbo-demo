package com.siemens.cbo;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.util.ReflectUtil;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;

/**
 * @author zjjfly[https://github.com/zjjfly] on 2020/8/26
 */
@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CboTest {

    static RelWriter rw;

    static RelNode opTree;

    @BeforeClass
    public static void init() {
        SchemaPlus rootSchema = CalciteSchema.createRootSchema(true).plus();
        FrameworkConfig config = Frameworks.newConfigBuilder()
                                           .defaultSchema(
                                                   CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
                                           .build();
        RelBuilder builder = RelBuilder.create(config);
        opTree = builder.scan("emps")
                        .scan("depts")
                        .join(JoinRelType.INNER, "deptno")
                        .filter(builder.call(GREATER_THAN, builder.field("empid"), builder.literal(100)))
                        .build();

        rw = new RelWriterImpl(new PrintWriter(System.out, true));
        log.info("logical algebras:");
        opTree.explain(rw);
        RelToSqlConverter visit = new RelToSqlConverter(MysqlSqlDialect.DEFAULT);
        final ReflectUtil.MethodDispatcher<SqlImplementor.Result> dispatcher = ReflectUtil.createMethodDispatcher(
                SqlImplementor.Result.class, visit, "visit",
                RelNode.class);
        SqlImplementor.Result result = dispatcher.invoke(opTree);
        log.info("sql:\n{}", result.asSelect().toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void test1HepPlanner() {
        HepProgram program = HepProgram.builder().addRuleInstance(FilterJoinRule.FILTER_ON_JOIN).build();
        HepPlanner hepPlanner = new HepPlanner(program);
        hepPlanner.setRoot(opTree);
        log.info("RBO result:");
        hepPlanner.findBestExp().explain(rw);
    }


    @Test
    public void test2VolcanoPlanner() throws SQLException {
        RelOptCluster cluster = opTree.getCluster();
        VolcanoPlanner planner = (VolcanoPlanner) cluster.getPlanner();
        List<RelOptRule> rules = planner.getRules();
        log.info("rules count:" + rules.size());
        rules.forEach(relOptRule -> {
            log.info(relOptRule.toString());
        });
        RelTraitSet desiredTraits = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
        RelNode newRoot = planner.changeTraits(opTree, desiredTraits);
        planner.setRoot(newRoot);

        RelNode optimized = planner.findBestExp();
        log.info("CBO result:");
        optimized.explain(rw);
        ResultSet result = RelRunners.run(optimized).executeQuery();
        ResultSetMetaData metaData = result.getMetaData();
        int columns = metaData.getColumnCount();
        String columnNames = Stream.iterate(1, i -> i + 1).limit(columns).map(column -> {
            try {
                return metaData.getColumnName(column);
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
            return null;
        }).collect(Collectors.joining(" "));
        log.info(columnNames);
        while (result.next()) {
            String vals = Stream.iterate(1, i -> i + 1).limit(columns).map(column -> {
                try {
                    return result.getString(column);
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
                return null;
            }).collect(Collectors.joining(" "));
            log.info(vals);
        }
    }

}
