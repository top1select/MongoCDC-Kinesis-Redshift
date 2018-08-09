package com.sms.cdc.redshift.oplog;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sms.bigdata.seagull.model.OP;
import com.sms.bigdata.seagull.model.Oplog;
import com.sms.cdc.redshift.Parser;
import com.sms.cdc.redshift.Utils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.Op;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class OplogParser implements Parser<String> {

    protected static final String _ID = "_id";
    protected static final String $OID = "$oid";

    protected Oplog oplog;

    public OplogParser(Oplog oplog) {
        this.oplog = oplog;
    }

    @Override
    public Parser<String> withTableName(String name) {
        if (StringUtils.isNotBlank(name)) {
            Oplog clone = SerializationUtils.clone(oplog);
            String[] newNsArr = name.split("\\.");
            String[] nsArr = oplog.getNamespace().split("\\.");
            if (newNsArr.length == 1) {
                nsArr[1] = newNsArr[0];
            } else if (newNsArr.length == 2) {
                nsArr[0] = newNsArr[0];
                nsArr[0] = newNsArr[1];
            }
            clone.setNamespace(nsArr[0] + "." + nsArr[1]);
            this.oplog = clone;

        }
        return this;
    }

    protected String insertOrUpdateToJson() {
        assert oplog.getOp() == OP.insert || oplog.getOp() == OP.update : "not valid oplog ";
        final Map<String, Object> rootDoc = oplog.getDocument();
        final Map<String, String> newDoc = Maps.newLinkedHashMap();
        for (Map.Entry<String, Object> entry : rootDoc.entrySet()) {
            String newVal = extractVal(entry.getValue());
            String key = entry.getKey().toLowerCase();
            newDoc.put(key, newVal);

        }
        return JSON.toJSONString(newDoc);
    }

    protected String extractVal(Object value) {
        if (value == null)
            return null;

        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            List<String> values = new ArrayList<>();
            map.forEach((k, v) -> {
                List<String> lst = extractList(v);
                if (k.equalsIgnoreCase("$date")) {
                    lst.stream().map(Utils::formatDate).forEach(values::add);
                } else {
                    values.addAll(lst);
                }
            });
            return Utils.subString65535(StringUtils.join(values, ","));
        }
        return Utils.subString65535(value.toString());

    }

    protected List<String> extractList(Object value) {
        if (value == null)
            return Lists.newArrayList();
        if (value instanceof Map) {
            Map<String,Object> map = (Map<String, Object>) value;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                entry.setValue(extractList(entry.getValue()));
            }
            return map.values().stream().flatMap(x -> ((List<String>) x).stream()).collect(Collectors.toList());
        }
        return Lists.newArrayList(value.toString());
    }

}


