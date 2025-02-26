// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SelectStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.View;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ColumnAliasGenerator;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.TableAliasGenerator;
import com.starrocks.common.UserException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rewrite.ExprRewriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
    private static final Logger LOG = LogManager.getLogger(SelectStmt.class);
    private UUID id = UUIDUtil.genUUID();

    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    protected SelectList selectList;
    private final ArrayList<String> colLabels; // lower case column labels
    protected final FromClause fromClause_;
    protected GroupByClause groupByClause;
    private List<Expr> originalExpr;
    //
    private Expr havingClause;  // original having clause
    protected Expr whereClause;
    // havingClause with aliases and agg output resolved
    private Expr havingPred;

    // set if we have any kind of aggregation operation, include SELECT DISTINCT
    private AggregateInfo aggInfo;
    // set if we have analytic function
    private AnalyticInfo analyticInfo;
    // substitutes all exprs in this select block to reference base tables
    // directly
    private ExprSubstitutionMap baseTblSmap = new ExprSubstitutionMap();

    private ValueList valueList;

    // if we have grouping extensions like cube or rollup or grouping sets
    private GroupingInfo groupingInfo;

    // having clause which has been analyzed
    // For example: select k1, sum(k2) a from t group by k1 having a>1;
    // this parameter: sum(t.k2) > 1
    private Expr havingClauseAfterAnaylzed;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    // SQL string of this SelectStmt before inline-view expression substitution.
    // Set in analyze().
    protected String sqlString_;

    // Table alias generator used during query rewriting.
    private TableAliasGenerator tableAliasGenerator = null;

    public SelectStmt(ValueList valueList, ArrayList<OrderByElement> orderByElement, LimitElement limitElement) {
        super(orderByElement, limitElement);
        this.valueList = valueList;
        this.selectList = new SelectList();
        this.fromClause_ = new FromClause();
        this.colLabels = Lists.newArrayList();
    }

    public SelectStmt(
            SelectList selectList,
            FromClause fromClause,
            Expr wherePredicate,
            GroupByClause groupByClause,
            Expr havingPredicate,
            ArrayList<OrderByElement> orderByElements,
            LimitElement limitElement) {
        super(orderByElements, limitElement);
        this.selectList = selectList;
        if (fromClause == null) {
            fromClause_ = new FromClause();
        } else {
            fromClause_ = fromClause;
        }
        this.whereClause = wherePredicate;
        this.groupByClause = groupByClause;
        this.havingClause = havingPredicate;

        this.colLabels = Lists.newArrayList();
        this.havingPred = null;
        this.aggInfo = null;
        this.sortInfo = null;
        this.groupingInfo = null;
    }

    protected SelectStmt(SelectStmt other) {
        super(other);
        this.id = other.id;
        selectList = other.selectList.clone();
        fromClause_ = other.fromClause_.clone();
        whereClause = (other.whereClause != null) ? other.whereClause.clone() : null;
        groupByClause = (other.groupByClause != null) ? other.groupByClause.clone() : null;
        havingClause = (other.havingClause != null) ? other.havingClause.clone() : null;

        colLabels = Lists.newArrayList(other.colLabels);
        aggInfo = (other.aggInfo != null) ? other.aggInfo.clone() : null;
        analyticInfo = (other.analyticInfo != null) ? other.analyticInfo.clone() : null;
        sqlString_ = (other.sqlString_ != null) ? other.sqlString_ : null;
        baseTblSmap = other.baseTblSmap.clone();
        groupingInfo = null;
    }

    @Override
    public void reset() {
        super.reset();
        selectList.reset();
        colLabels.clear();
        fromClause_.reset();
        if (whereClause != null) {
            whereClause.reset();
        }
        if (groupByClause != null) {
            groupByClause.reset();
        }
        if (havingClause != null) {
            havingClause.reset();
        }
        havingClauseAfterAnaylzed = null;
        havingPred = null;
        aggInfo = null;
        analyticInfo = null;
        baseTblSmap.clear();
        groupingInfo = null;
    }

    @Override
    public QueryStmt clone() {
        return new SelectStmt(this);
    }

    public UUID getId() {
        return id;
    }

    /**
     * @return the original select list items from the query
     */
    public SelectList getSelectList() {
        return selectList;
    }

    public void setSelectList(SelectList selectList) {
        this.selectList = selectList;
    }

    public ValueList getValueList() {
        return valueList;
    }

    public List<TableRef> getTableRefs() {
        return fromClause_.getTableRefs();
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(Expr whereClause) {
        this.whereClause = whereClause;
    }

    public GroupByClause getGroupByClause() {
        return groupByClause;
    }

    public boolean hasAnalyticInfo() {
        return analyticInfo != null;
    }

    @Override
    public ArrayList<String> getColLabels() {
        return colLabels;
    }

    @Override
    public void getDbs(ConnectContext context, Map<String, Database> dbs) throws AnalysisException {
        getWithClauseDbs(context, dbs);
        for (TableRef tblRef : fromClause_) {
            if (tblRef instanceof InlineViewRef) {
                // Inline view reference
                QueryStmt inlineStmt = ((InlineViewRef) tblRef).getViewStmt();
                inlineStmt.withClause_ = this.withClause_;
                inlineStmt.getDbs(context, dbs);
            } else if (tblRef instanceof FunctionTableRef) {
                //FunctionTableRef dbName is empty
                continue;
            } else {
                String dbName = tblRef.getName().getDb();
                if (Strings.isNullOrEmpty(dbName)) {
                    dbName = context.getDatabase();
                } else {
                    dbName = ClusterNamespace.getFullName(context.getClusterName(), tblRef.getName().getDb());
                }
                if (withClause_ != null && isViewTableRef(tblRef)) {
                    continue;
                }
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }

                Database db = context.getGlobalStateMgr().getDb(dbName);
                if (db == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                }

                // check auth
                if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                        tblRef.getName().getTbl(),
                        PrivPredicate.SELECT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                            ConnectContext.get().getQualifiedUser(),
                            ConnectContext.get().getRemoteIP(),
                            tblRef.getName().getTbl());
                }

                dbs.put(dbName, db);
            }
        }
    }

    private boolean isViewTableRef(TableRef tblRef) {
        List<View> views = withClause_.getViews();
        for (View view : views) {
            if (view.getName().equals(tblRef.getName().toString())) {
                return true;
            }
        }
        return false;
    }

    // Column alias generator used during query rewriting.
    private ColumnAliasGenerator columnAliasGenerator = null;

    public ColumnAliasGenerator getColumnAliasGenerator() {
        if (columnAliasGenerator == null) {
            columnAliasGenerator = new ColumnAliasGenerator(colLabels, null);
        }
        return columnAliasGenerator;
    }

    public TableAliasGenerator getTableAliasGenerator() {
        if (tableAliasGenerator == null) {
            tableAliasGenerator = new TableAliasGenerator(analyzer, null);
        }
        return tableAliasGenerator;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
    }

    public List<TupleId> getTableRefIds() {
        List<TupleId> result = Lists.newArrayList();

        for (TableRef ref : fromClause_) {
            result.add(ref.getId());
        }

        return result;
    }

    @Override
    public List<TupleId> collectTupleIds() {
        List<TupleId> result = Lists.newArrayList();
        resultExprs.stream().forEach(expr -> expr.getIds(result, null));
        result.addAll(getTableRefIds());
        if (whereClause != null) {
            whereClause.getIds(result, null);
        }
        if (havingClauseAfterAnaylzed != null) {
            havingClauseAfterAnaylzed.getIds(result, null);
        }
        return result;
    }

    /**
     * Marks all unassigned join predicates as well as exprs in aggInfo and sortInfo.
     */
    public void materializeRequiredSlots(Analyzer analyzer) throws AnalysisException {
        // Mark unassigned join predicates. Some predicates that must be evaluated by a join
        // can also be safely evaluated below the join (picked up by getBoundPredicates()).
        // Such predicates will be marked twice and that is ok.
        List<Expr> unassigned =
                analyzer.getUnassignedConjuncts(getTableRefIds(), true);
        List<Expr> unassignedJoinConjuncts = Lists.newArrayList();
        for (Expr e : unassigned) {
            if (analyzer.evalAfterJoin(e)) {
                unassignedJoinConjuncts.add(e);
            }
        }
        List<Expr> baseTblJoinConjuncts =
                Expr.trySubstituteList(unassignedJoinConjuncts, baseTblSmap, analyzer, false);
        analyzer.materializeSlots(baseTblJoinConjuncts);

        if (evaluateOrderBy) {
            // mark ordering exprs before marking agg/analytic exprs because they could contain
            // agg/analytic exprs that are not referenced anywhere but the ORDER BY clause
            sortInfo.materializeRequiredSlots(analyzer, baseTblSmap);
        }

        if (hasAnalyticInfo()) {
            // Mark analytic exprs before marking agg exprs because they could contain agg
            // exprs that are not referenced anywhere but the analytic expr.
            // Gather unassigned predicates and mark their slots. It is not desirable
            // to account for propagated predicates because if an analytic expr is only
            // referenced by a propagated predicate, then it's better to not materialize the
            // analytic expr at all.
            ArrayList<TupleId> tids = Lists.newArrayList();
            getMaterializedTupleIds(tids); // includes the analytic tuple
            List<Expr> conjuncts = analyzer.getUnassignedConjuncts(tids);
            analyzer.materializeSlots(conjuncts);
            analyticInfo.materializeRequiredSlots(analyzer, baseTblSmap);
        }

        if (aggInfo != null) {
            // mark all agg exprs needed for HAVING pred and binding predicates as materialized
            // before calling AggregateInfo.materializeRequiredSlots(), otherwise they won't
            // show up in AggregateInfo.getMaterializedAggregateExprs()
            ArrayList<Expr> havingConjuncts = Lists.newArrayList();
            if (havingPred != null) {
                havingConjuncts.add(havingPred);
            }

            havingConjuncts.addAll(
                    analyzer.getUnassignedConjuncts(aggInfo.getResultTupleId().asList()));
            materializeSlots(analyzer, havingConjuncts);
            aggInfo.materializeRequiredSlots(analyzer, baseTblSmap);
        }
    }

    /**
     * Expand "*" select list item.
     */
    private void expandStar(Analyzer analyzer) throws AnalysisException {
        if (fromClause_.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        // expand in From clause order
        for (TableRef tableRef : fromClause_) {
            if (analyzer.isSemiJoined(tableRef.getId())) {
                continue;
            }
            expandStar(tableRef.getAliasAsName(), tableRef.getDesc());
        }
    }

    /**
     * Expand "<tbl>.*" select list item.
     */
    private void expandStar(Analyzer analyzer, TableName tblName) throws AnalysisException {
        Collection<TupleDescriptor> descs = analyzer.getDescriptor(tblName);
        if (descs == null || descs.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName.getTbl());
        }
        for (TupleDescriptor desc : descs) {
            expandStar(tblName, desc);
        }
    }

    /**
     * Expand "*" for a particular tuple descriptor by appending
     * refs for each column to selectListExprs.
     */
    private void expandStar(TableName tblName, TupleDescriptor desc) {
        for (Column col : desc.getTable().getBaseSchema()) {
            resultExprs.add(new SlotRef(tblName, col.getName()));
            colLabels.add(col.getName());
        }
    }

    @Override
    public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
        Preconditions.checkState(isAnalyzed());
        rewriteSelectList(rewriter);
        for (TableRef ref : fromClause_) {
            ref.rewriteExprs(rewriter, analyzer);
        }
        // Also equal exprs in the statements of subqueries.
        List<Subquery> subqueryExprs = Lists.newArrayList();
        if (whereClause != null) {
            whereClause = rewriter.rewrite(whereClause, analyzer);
            whereClause.collect(Subquery.class, subqueryExprs);

        }
        if (havingClause != null) {
            havingClause = rewriter.rewrite(havingClause, analyzer);
            havingClauseAfterAnaylzed.collect(Subquery.class, subqueryExprs);
        }
        for (Subquery subquery : subqueryExprs) {
            subquery.getStatement().rewriteExprs(rewriter);
        }
        if (groupByClause != null) {
            ArrayList<Expr> groupingExprs = groupByClause.getGroupingExprs();
            if (groupingExprs != null) {
                rewriter.rewriteList(groupingExprs, analyzer);
            }
            List<Expr> oriGroupingExprs = groupByClause.getOriGroupingExprs();
            if (oriGroupingExprs != null) {
                rewriter.rewriteList(oriGroupingExprs, analyzer);
            }
        }
        if (orderByElements != null) {
            for (OrderByElement orderByElem : orderByElements) {
                orderByElem.setExpr(rewriter.rewrite(orderByElem.getExpr(), analyzer));
            }
        }
    }

    private void rewriteSelectList(ExprRewriter rewriter) throws AnalysisException {
        for (SelectListItem item : selectList.getItems()) {
            if (item.getExpr() instanceof CaseExpr && item.getExpr().contains(Predicates.instanceOf(Subquery.class))) {
                rewriteSubquery(item.getExpr(), analyzer);
            }
        }
        selectList.rewriteExprs(rewriter, analyzer);
    }

    /**
     * equal subquery in case when to an inline view
     * subquery in case when statement like
     * <p>
     * SELECT CASE
     * WHEN (
     * SELECT COUNT(*) / 2
     * FROM t
     * ) > k4 THEN (
     * SELECT AVG(k4)
     * FROM t
     * )
     * ELSE (
     * SELECT SUM(k4)
     * FROM t
     * )
     * END AS kk4
     * FROM t;
     * this statement will be equal to
     * <p>
     * SELECT CASE
     * WHEN t1.a > k4 THEN t2.a
     * ELSE t3.a
     * END AS kk4
     * FROM t, (
     * SELECT COUNT(*) / 2 AS a
     * FROM t
     * ) t1,  (
     * SELECT AVG(k4) AS a
     * FROM t
     * ) t2,  (
     * SELECT SUM(k4) AS a
     * FROM t
     * ) t3;
     */
    private Expr rewriteSubquery(Expr expr, Analyzer analyzer)
            throws AnalysisException {
        if (expr instanceof Subquery) {
            if (!(((Subquery) expr).getStatement() instanceof SelectStmt)) {
                throw new AnalysisException("Only support select subquery in case-when clause.");
            }
            if (expr.isCorrelatedPredicate(getTableRefIds())) {
                throw new AnalysisException("The correlated subquery in case-when clause is not supported");
            }
            SelectStmt subquery = (SelectStmt) ((Subquery) expr).getStatement();
            if (subquery.resultExprs.size() != 1 || !subquery.returnsSingleRow()) {
                throw new AnalysisException("Subquery in case-when must return scala type");
            }
            subquery.reset();
            subquery.setAssertNumRowsElement(1, AssertNumRowsElement.Assertion.EQ);
            String alias = getTableAliasGenerator().getNextAlias();
            String colAlias = getColumnAliasGenerator().getNextAlias();
            InlineViewRef inlineViewRef = new InlineViewRef(alias, subquery, Arrays.asList(colAlias));
            try {
                inlineViewRef.analyze(analyzer);
            } catch (UserException e) {
                throw new AnalysisException(e.getMessage());
            }
            fromClause_.add(inlineViewRef);
            expr = new SlotRef(inlineViewRef.getAliasAsName(), colAlias);
        } else if (CollectionUtils.isNotEmpty(expr.getChildren())) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, rewriteSubquery(expr.getChild(i), analyzer));
            }
        }
        return expr;
    }

    @Override
    public String toSql() {
        if (sqlString_ != null) {
            return sqlString_;
        }
        StringBuilder strBuilder = new StringBuilder();
        if (withClause_ != null) {
            strBuilder.append(withClause_.toSql());
            strBuilder.append(" ");
        }

        // Select list
        strBuilder.append("SELECT ");
        if (selectList.isDistinct()) {
            strBuilder.append("DISTINCT ");
        }

        if (needToSql && originalExpr == null) {
            originalExpr = Expr.cloneList(resultExprs);
        }

        if (!resultExprs.isEmpty()) {
            if (needToSql) {
                for (int i = 0; i < originalExpr.size(); ++i) {
                    if (i != 0) {
                        strBuilder.append(", ");
                    }
                    strBuilder.append(originalExpr.get(i).toSql());
                    strBuilder.append(" AS ").append(SqlUtils.getIdentSql(colLabels.get(i)));
                }
            } else {
                for (int i = 0; i < resultExprs.size(); ++i) {
                    if (i != 0) {
                        strBuilder.append(", ");
                    }
                    strBuilder.append(resultExprs.get(i).toSql());
                    strBuilder.append(" AS ").append(SqlUtils.getIdentSql(colLabels.get(i)));
                }
            }
        } else {
            // For subQuery, the resultExprs is empty, we need to use selectList
            for (int i = 0; i < selectList.getItems().size(); ++i) {
                if (i != 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append(selectList.getItems().get(i).toSql());
            }
        }

        // From clause
        if (!fromClause_.isEmpty()) {
            strBuilder.append(fromClause_.toSql());
        }

        // Where clause
        if (whereClause != null) {
            strBuilder.append(" WHERE ");
            strBuilder.append(whereClause.toSql());
        }
        // Group By clause
        if (groupByClause != null) {
            strBuilder.append(" GROUP BY ");
            strBuilder.append(groupByClause.toSql());
        }
        // Having clause
        if (havingClause != null) {
            strBuilder.append(" HAVING ");
            strBuilder.append(havingClause.toSql());
        }
        // Order By clause
        if (orderByElements != null) {
            strBuilder.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                strBuilder.append(orderByElements.get(i).getExpr().toSql());
                if (sortInfo != null) {
                    strBuilder.append((sortInfo.getIsAscOrder().get(i)) ? " ASC" : " DESC");
                }
                strBuilder.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }
        // Limit clause.
        if (hasLimitClause()) {
            strBuilder.append(limitElement.toSql());
        }

        if (hasOutFileClause()) {
            strBuilder.append(outFileClause.toSql());
        }
        return strBuilder.toString();
    }

    @Override
    public String toDigest() {
        StringBuilder strBuilder = new StringBuilder();
        if (withClause_ != null) {
            strBuilder.append(withClause_.toDigest());
            strBuilder.append(" ");
        }

        // Select list
        strBuilder.append("select ");
        if (selectList.isDistinct()) {
            strBuilder.append("distinct ");
        }

        if (originalExpr == null) {
            originalExpr = Expr.cloneList(resultExprs);
        }

        if (resultExprs.isEmpty()) {
            for (int i = 0; i < selectList.getItems().size(); ++i) {
                if (i != 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append(selectList.getItems().get(i).toDigest());
            }
        } else {
            for (int i = 0; i < originalExpr.size(); ++i) {
                if (i != 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append(originalExpr.get(i).toDigest());
                strBuilder.append(" as ").append(SqlUtils.getIdentSql(colLabels.get(i)));
            }
        }

        // From clause
        if (!fromClause_.isEmpty()) {
            strBuilder.append(fromClause_.toDigest());
        }

        // Where clause
        if (whereClause != null) {
            strBuilder.append(" where ");
            strBuilder.append(whereClause.toDigest());
        }
        // Group By clause
        if (groupByClause != null) {
            strBuilder.append(" group by ");
            strBuilder.append(groupByClause.toSql());
        }
        // Having clause
        if (havingClause != null) {
            strBuilder.append(" having ");
            strBuilder.append(havingClause.toDigest());
        }
        // Order By clause
        if (orderByElements != null) {
            strBuilder.append(" order by ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                strBuilder.append(orderByElements.get(i).getExpr().toDigest());
                if (sortInfo != null) {
                    strBuilder.append((sortInfo.getIsAscOrder().get(i)) ? " asc" : " desc");
                }
                strBuilder.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }
        // Limit clause.
        if (hasLimitClause()) {
            strBuilder.append(limitElement.toDigest());
        }

        return strBuilder.toString();
    }

    /**
     * If the select statement has a sort/top that is evaluated, then the sort tuple
     * is materialized. Else, if there is aggregation then the aggregate tuple id is
     * materialized. Otherwise, all referenced tables are materialized as long as they are
     * not semi-joined. If there are analytics and no sort, then the returned tuple
     * ids also include the logical analytic output tuple.
     */
    @Override
    public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
        // If select statement has an aggregate, then the aggregate tuple id is materialized.
        // Otherwise, all referenced tables are materialized.
        if (evaluateOrderBy) {
            tupleIdList.add(sortInfo.getSortTupleDescriptor().getId());
        } else if (aggInfo != null) {
            // Return the tuple id produced in the final aggregation step.
            if (aggInfo.isDistinctAgg()) {
                tupleIdList.add(aggInfo.getSecondPhaseDistinctAggInfo().getOutputTupleId());
            } else {
                tupleIdList.add(aggInfo.getOutputTupleId());
            }
        } else {
            for (TableRef tblRef : fromClause_) {
                tupleIdList.addAll(tblRef.getMaterializedTupleIds());
            }
        }
        // Fixme(kks): get tuple id from analyticInfo is wrong, should get from AnalyticEvalNode
        // We materialize the agg tuple or the table refs together with the analytic tuple.
        if (hasAnalyticInfo() && isEvaluateOrderBy()) {
            tupleIdList.add(analyticInfo.getOutputTupleId());
        }
    }

    @Override
    public void substituteSelectList(Analyzer analyzer, List<String> newColLabels)
            throws AnalysisException, UserException {
        // analyze with clause
        if (hasWithClause()) {
            withClause_.analyze(analyzer);
        }
        // start out with table refs to establish aliases
        TableRef leftTblRef = null;  // the one to the left of tblRef
        for (int i = 0; i < fromClause_.size(); ++i) {
            // Resolve and replace non-InlineViewRef table refs with a BaseTableRef or ViewRef.
            TableRef tblRef = fromClause_.get(i);
            tblRef = analyzer.resolveTableRef(tblRef);
            Preconditions.checkNotNull(tblRef);
            fromClause_.set(i, tblRef);
            tblRef.setLeftTblRef(leftTblRef);
            tblRef.analyze(analyzer);
            leftTblRef = tblRef;
        }
        // populate selectListExprs, aliasSMap, and colNames
        for (SelectListItem item : selectList.getItems()) {
            if (item.isStar()) {
                TableName tblName = item.getTblName();
                if (tblName == null) {
                    expandStar(analyzer);
                } else {
                    expandStar(analyzer, tblName);
                }
            } else {
                // to make sure the sortinfo's AnalyticExpr and resultExprs's AnalyticExpr analytic once
                if (item.getExpr() instanceof AnalyticExpr) {
                    item.getExpr().analyze(analyzer);
                }
                if (item.getAlias() != null) {
                    SlotRef aliasRef = new SlotRef(null, item.getAlias());
                    SlotRef newAliasRef = new SlotRef(null, newColLabels.get(resultExprs.size()));
                    newAliasRef.analysisDone();
                    aliasSMap.put(aliasRef, newAliasRef);
                }
                resultExprs.add(item.getExpr());
            }
        }
        // substitute group by
        if (groupByClause != null) {
            substituteOrdinalsAliases(groupByClause.getGroupingExprs(), "GROUP BY", analyzer);
        }
        // substitute having
        if (havingClause != null) {
            havingClause = havingClause.clone(aliasSMap);
        }
        // substitute order by
        if (orderByElements != null) {
            for (int i = 0; i < orderByElements.size(); ++i) {
                orderByElements = OrderByElement.substitute(orderByElements, aliasSMap, analyzer);
            }
        }

        colLabels.clear();
        colLabels.addAll(newColLabels);
    }

    @Override
    public void substituteSelectListForCreateView(Analyzer analyzer, List<String> newColLabels)
            throws UserException {
        resultExprs = Lists.newArrayList();
        substituteSelectList(analyzer, newColLabels);
        sqlString_ = null;
    }

    public boolean hasAggInfo() {
        return aggInfo != null;
    }

    public boolean hasGroupByClause() {
        return groupByClause != null;
    }

    /**
     * Check if the stmt returns a single row. This can happen
     * in the following cases:
     * 1. select stmt with a 'limit 1' clause
     * 2. select stmt with an aggregate function and no group by.
     * 3. select stmt with no from clause.
     * <p>
     * This function may produce false negatives because the cardinality of the
     * result set also depends on the data a stmt is processing.
     */
    public boolean returnsSingleRow() {
        // limit 1 clause
        if (hasLimitClause() && getLimit() == 1) {
            return true;
        }
        // No from clause (base tables or inline views)
        if (fromClause_.isEmpty()) {
            return true;
        }
        // Aggregation with no group by and no DISTINCT
        if (hasAggInfo() && !hasGroupByClause() && !selectList.isDistinct()) {
            return true;
        }
        // In all other cases, return false.
        return false;
    }

    @Override
    public void collectTableRefs(List<TableRef> tblRefs) {
        for (TableRef tblRef : fromClause_) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
                inlineViewRef.getViewStmt().collectTableRefs(tblRefs);
            } else {
                tblRefs.add(tblRef);
            }
        }
    }

    private boolean checkGroupingFn(Expr expr) {
        if (expr instanceof GroupingFunctionCallExpr) {
            return true;
        } else if (expr.getChildren() != null && expr.getChildren().size() > 0) {
            for (Expr child : expr.getChildren()) {
                if (checkGroupingFn(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SelectStmt)) {
            return false;
        }
        return this.id.equals(((SelectStmt) obj).id);
    }
}
