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

public class DeleteParser extends Op{
}
