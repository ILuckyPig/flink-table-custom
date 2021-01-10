package com.lu.table.function;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.util.JsonUtils;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.Map;

@FunctionHint(output = @DataTypeHint("ROW<term STRING, pos INT, realFrequency INT>"))
public class WordVoiceFlatMap extends TableFunction<Row> {
    public void eval(String str) throws JsonProcessingException {
        JsonNode jsonNode = JsonUtils.MAPPER.readTree(str);
        Iterator<Map.Entry<String, JsonNode>> entryIterator = jsonNode.fields();
        while (entryIterator.hasNext()) {
            Map.Entry<String, JsonNode> next = entryIterator.next();
            String term = next.getKey();
            int freq = next.getValue().get("freq").asInt();
            int pos = next.getValue().get("senti_type").asInt();
            collect(Row.of(term, pos, freq));
        }
    }
}
