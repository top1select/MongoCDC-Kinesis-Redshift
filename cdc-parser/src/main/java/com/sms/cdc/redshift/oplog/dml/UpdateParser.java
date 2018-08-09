package com.sms.cdc.redshift.oplog.dml;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGUpdateStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.sms.bigdata.seagull.model.OP;
import com.sms.bigdata.seagull.model.Oplog;
import com.sms.cdc.redshift.Utils;
import com.sms.cdc.redshift.oplog.OplogParser;

import java.util.Map;

public class UpdateParser extends OplogParser {

    public UpdateParser(Oplog oplog) {
        super(oplog);
    }

    @Override
    public String toRedshift() {
        return toSql();
    }

    @Override
    public String toRedshift(boolean upsert) {
        if (upsert) {
            return insertOrUpdateToJson();
        } else {
            return toSql();
        }
    }

    private String toSql() {
        assert oplog.getOp() == OP.update : "not update operation";

        String[] nsArr = oplog.getNamespace().split("\\.");

        String idValue = getId(oplog);

        PGUpdateStatement updateStmt = new PGUpdateStatement();

        Map<String, Object> $set = getSetMap(oplog);

        $set.forEach((k, v) -> {
            SQLIdentifierExpr column = new SQLIdentifierExpr();
            column.setName("\"" + k + "\"");

            SQLExpr expr = createSQLExpr(v);

            SQLUpdateSetItem item = new SQLUpdateSetItem();
            item.setColumn(column);
            item.setValue(expr);

            updateStmt.addItem(item);

        });

        SQLIdentifierExpr left = new SQLIdentifierExpr();
        left.setName(_ID);

        SQLCharExpr right = new SQLCharExpr();
        right.setText(idValue);

        SQLBinaryOpExpr whereExpr = new SQLBinaryOpExpr();

        whereExpr.setLeft(left);
        whereExpr.setRight(right);
        whereExpr.setOperator(SQLBinaryOperator.Equality);
        whereExpr.setDbType(JdbcConstants.POSTGRESQL);

        updateStmt.setWhere(whereExpr);
        updateStmt.setTableSource(new SQLExprTableSource(new SQLPropertyExpr(nsArr[0], nsArr[1])));
        updateStmt.setDbType(JdbcConstants.POSTGRESQL);

        return SQLUtils.toPGString(updateStmt, Utils.formatOption());


    }

    private String getId(Oplog oplog) {
        Object idObj = oplog.getCriteria().get(_ID);
        if (idObj instanceof Map) {
            return ((Map<String, String>) idObj).get($OID);
        }
        return idObj.toString();
    }

    private Map<String, Object> getSetMap(Oplog oplog) {
        Map<String, Object> document = oplog.getDocument();
        if (document.size() == 1 && document.containsKey("$set")) {
            return (Map<String, Object>) document.get("$set");
        } else {
            return document;
        }
    }

    private SQLExpr createSQLExpr(Object value) {
        if (value == null) {
            return new SQLNullExpr();
        }
        else if (value instanceof Map) {
            String mapValue = getMapValue(value);
            SQLCharExpr expr = new SQLCharExpr();
            expr.setText(mapValue);
            return expr;
        }
        else {
            SQLCharExpr expr = new SQLCharExpr();
            expr.setText(Utils.subString65535(value.toString()));
            return expr;
        }
    }

    private String getMapValue(Object value) {
        Map<String, Object> map = (Map<String, Object>) value;
        if (map.size() > 1) {
            return Utils.subString65535(JSON.toJSONString(map));
        }
        else {
            if (map.containsKey("$date")) {
                Object $date = map.get("$date");
                if ($date instanceof Long) {
                    return Utils.formatDate((Long) $date);
                }
                return Utils.formatDate($date.toString());
            }
            return Lists.newArrayList(map.values()).get(0).toString();
        }
    }

}
