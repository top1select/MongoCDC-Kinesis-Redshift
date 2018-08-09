package com.sms.cdc.redshift.binlog;

import com.alibaba.druid.sql.ast.SQLExpr;

import java.util.List;
import java.util.Optional;

public class RedshiftDataType {

    private String name;
    private Optional<List<SQLExpr>> arguments = Optional.empty();

    public RedshiftDataType() {
    }

    public RedshiftDataType(String name) {
        this.name = name;
    }

    public RedshiftDataType(String name, Optional<List<SQLExpr>> arguments) {
        this.name = name;
        this.arguments = arguments;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Optional<List<SQLExpr>> getArguments() {
        return arguments;
    }

    public void setArguments(Optional<List<SQLExpr>> arguments) {
        this.arguments = arguments;
    }
}
