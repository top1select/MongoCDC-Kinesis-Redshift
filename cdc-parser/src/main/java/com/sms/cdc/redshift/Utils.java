package com.sms.cdc.redshift;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.util.JdbcConstants;
import com.sms.cdc.redshift.binlog.RedshiftDataType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.tools.cmd.Opt;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Utils {

    /**
     * Redshift Reserved words list
     */
    public static final List<String> REDSHIFT_KEYWORDS = Arrays.asList(
            "AES128", "AES256", "ALL", "ALLOWOVERWRITE", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC",
            "AUTHORIZATION", "BACKUP", "BETWEEN", "BINARY", "BLANKSASNULL", "BOTH", "BYTEDICT", "BZIP2", "CASE", "CAST",
            "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE", "CREDENTIALS", "CROSS", "CURRENT_DATE", "CURRENT_TIME",
            "CURRENT_TIMESTAMP", "CURRENT_USER", "CURRENT_USER_ID", "DEFAULT", "DEFERRABLE", "DEFLATE", "DEFRAG", "DELTA",
            "DELTA32K", "DESC", "DISABLE", "DISTINCT", "DO", "ELSE", "EMPTYASNULL", "ENABLE", "ENCODE", "ENCRYPT", "ENCRYPTION",
            "END", "EXCEPT", "EXPLICIT", "FALSE", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GLOBALDICT256", "GLOBALDICT64K",
            "GRANT", "GROUP", "GZIP", "HAVING", "IDENTITY", "IGNORE", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO",
            "IS", "ISNULL", "JOIN", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "LUN", "LUNS", "LZO",
            "LZOP", "MINUS", "MOSTLY13", "MOSTLY32", "MOSTLY8", "NATURAL", "NEW", "NOT", "NOTNULL", "NULL", "NULLS", "OFF",
            "OFFLINE", "OFFSET", "OID", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUTER", "OVERLAPS", "PARALLEL", "PARTITION",
            "PERCENT", "PERMISSIONS", "PLACING", "PRIMARY", "RAW", "READRATIO", "RECOVER", "REFERENCES", "RESPECT", "REJECTLOG",
            "RESORT", "RESTORE", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SNAPSHOT", "SOME", "SYSDATE", "SYSTEM", "TABLE",
            "TAG", "TDES", "TEXT255", "TEXT32K", "THEN", "TIMESTAMP", "TO", "TOP", "TRAILING", "TRUE", "TRUNCATECOLUMNS",
            "UNION", "UNIQUE", "USER", "USING", "VERBOSE", "WALLET", "WHEN", "WHERE", "WITH", "WITHOUT");
    private static final DateTimeZone zoneUTC = DateTimeZone.UTC;
    private static final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public static SQLDataTypeImpl convertDataType(SQLDataTypeImpl dataType) {
        final SQLDataTypeImpl clone = dataType.clone();

        clone.getArguments().clear();
        clone.setDbType(JdbcConstants.POSTGRESQL);

        final String name = dataType.getName();
        final boolean unsigned = dataType.isUnsigned();
        final List<SQLExpr> arguments = dataType.getArguments();

        final RedshiftDataType redshiftDataType = mysqlDataTypeReplace(name, unsigned, arguments);
        clone.setName(redshiftDataType.getName());
        redshiftDataType.getArguments().ifPresent(c -> c.forEach(clone::addArgument));

        return clone;
    }

    public static RedshiftDataType mysqlDataTypeReplace(String mysqlDataType, boolean unsigned, List<SQLExpr> arguments) {
        switch (mysqlDataType.toLowerCase()) {
            case "int":
            case "integer":
                if (unsigned) return new RedshiftDataType("int8");
                else return new RedshiftDataType("int4");
            case "tinyint":
            case "smallint":
                if (unsigned) return new RedshiftDataType("int4");
                else return new RedshiftDataType("int2");
            case "bigint":
                return new RedshiftDataType("int8");
            case "decimal":
                return new RedshiftDataType("numeric", Optional.ofNullable(arguments));
            case "float":
                return new RedshiftDataType("float4");
            case "double":
                return new RedshiftDataType("float8");
            case "varchar":
            case "char":
                return new RedshiftDataType("varchar",
                        Optional.ofNullable(
                                // 大小增加3倍数
                                arguments.stream()
                                        .map(sqlExpr -> {
                                            SQLIntegerExpr integerExpr = (SQLIntegerExpr) sqlExpr;
                                            SQLIntegerExpr newExpr = integerExpr.clone();
                                            int three = integerExpr.getNumber().intValue() * 3;
                                            // Redshift varchar类型最大长度为65535
                                            newExpr.setNumber(Math.min(three, 65535));
                                            return newExpr;
                                        })
                                        .collect(Collectors.toList())));
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
                SQLIntegerExpr arg = new SQLIntegerExpr();
                arg.setNumber(65535);
                return new RedshiftDataType("varchar", Optional.of(Arrays.asList(arg)));
            case "time":
            case "year":
            case "geometry":
                return new RedshiftDataType("varchar");
            case "datetime":
                return new RedshiftDataType("timestamp");
            default:
                return new RedshiftDataType(mysqlDataType);
        }
    }

    public static SQLUtils.FormatOption formatOption() {
        final SQLUtils.FormatOption option = new SQLUtils.FormatOption();
        option.setPrettyFormat(false);
        return option;
    }

    public static SQLColumnDefinition createSqlColumnDefinition(SQLName name, SQLDataTypeImpl cloneDataType) {
        final SQLColumnDefinition def = new SQLColumnDefinition();
        def.setDbType(JdbcConstants.POSTGRESQL);
        def.setName(name);
        if (name instanceof SQLIdentifierExpr) {
            final SQLIdentifierExpr nameExpr = (SQLIdentifierExpr) name;
            final String newName = nameStringProcess(nameExpr.getName());
            nameExpr.setName(newName);
            def.setName(nameExpr);
        }
        def.setDataType(cloneDataType);
        return def;
    }


    private static String nameStringProcess(String nameStr) {

        if (nameStr.contains("`")) {
            return nameStr.replaceAll("`", "\"");
        }

        if (REDSHIFT_KEYWORDS.contains(nameStr.toUpperCase())) {
            return "\"" + nameStr + "\"";
        }

        return nameStr;
    }

    public static String formatDate(String lng) {
        return new DateTime(Long.parseLong(lng)).withZone(zoneUTC).toString(fmt);
    }

    public static String formatDate(Long lng) {
        return new DateTime(lng).withZone(zoneUTC).toString(fmt);
    }

    public static String formatDate(java.util.Date date) {
        return new DateTime(date).withZone(zoneUTC).toString(fmt);
    }

    public static String subString65535(String value) {
        return Optional.ofNullable(value)
                .filter(x -> x.length() > 16838)
                .map(x -> x.substring(0, 16379) + "...")
                .orElse(value);
    }

}
