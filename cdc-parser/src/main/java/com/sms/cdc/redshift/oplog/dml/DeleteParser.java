package com.sms.cdc.redshift.oplog.dml;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGDeleteStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSONObject;
import com.sms.bigdata.seagull.model.OP;
import com.sms.bigdata.seagull.model.Oplog;
import com.sms.cdc.redshift.Utils;
import com.sms.cdc.redshift.oplog.OplogParser;

import java.util.Map;

public class DeleteParser extends OplogParser{

    public DeleteParser(Oplog oplog) {
        super(oplog);
    }

    @Override
    public String toRedshift() {
        assert oplog.getOp() == OP.delete : "not delete";
        return toSql();
    }

    @Override
    public String toRedshift(boolean flag) {
        assert oplog.getOp() == OP.delete : "not delete";
        if (flag) {
            return toJson();
        }
        else {
            return toSql();
        }
    }

    private String toSql() {
        String[] nsArr = oplog.getNamespace().split("\\.");
        String idValue = getId(oplog);

        SQLIdentifierExpr left = new SQLIdentifierExpr();
        left.setName(_ID);

        SQLCharExpr right = new SQLCharExpr();
        right.setText(idValue);

        SQLBinaryOpExpr whereExpr = new SQLBinaryOpExpr();
        whereExpr.setLeft(left);
        whereExpr.setRight(right);
        whereExpr.setOperator(SQLBinaryOperator.Equality);
        whereExpr.setDbType(JdbcConstants.POSTGRESQL);

        PGDeleteStatement deleteStmt = new PGDeleteStatement();

        deleteStmt.setTableSource(new SQLExprTableSource(new SQLPropertyExpr(nsArr[0], nsArr[1])));
        deleteStmt.setWhere(whereExpr);
        deleteStmt.setDbType(JdbcConstants.POSTGRESQL);
        return SQLUtils.toPGString(deleteStmt, Utils.formatOption());
    }

    private String toJson() {
        String namespace = oplog.getNamespace();
        String id = getId(oplog);
        return new JSONObject(2)
                .fluentPut("_table_name", namespace)
                .fluentPut("_id", id)
                .toJSONString();
    }

    private String getId(Oplog oplog) {
        Object idObj = oplog.getDocument().get(_ID);
        if (idObj instanceof Map) {
            return ((Map<String, String>) idObj).get($OID);
        }
        return idObj.toString();
    }

}
