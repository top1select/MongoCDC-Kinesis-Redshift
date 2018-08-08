package com.sms.cdc.kinesis.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sms.bigdata.seagull.utils.Config;
import com.sms.cdc.kinesis.exception.CdcBatchException;
import com.sms.cdc.kinesis.exception.CdcCopyException;
import com.sms.cdc.kinesis.exception.CdcException;
import com.sms.cdc.kinesis.exception.CdcMergeTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class RedshiftUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RedshiftUtil.class);

    private static final String accessKeyId;
    private static final String secretAccessKey;
    final static int TABLE_NOT_FOUND_ERROR_CODE = 500310;
    final static int DEFAULT_VARCHAR_LENGTH = 1024;
    final static String s3KeyPattern = "access_key_id '[a-zA-Z]*'";
    final static String s3SecretPattern = "secret_access_key '[\\w|`~!@#$%^&*()+=|{}':;',\\\\[\\\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]*'";
    final static String[] TABLE_TYPE = {"TABLE"};

    static {
        Config cdcConfig = new Config("cdc.properties");
        accessKeyId = cdcConfig.get("access.key.id");
        secretAccessKey = cdcConfig.get("secret.access.key");
    }

    public static String copyCmd(String table, String s3Path) {
        return "copy " + table + " from '" + s3Path + "' " +
                "access_key_id '" + accessKeyId + "' " +
                "secret_access_key '" + secretAccessKey + "' " +
                "json 'auto' dateformat 'auto'";
    }

}
