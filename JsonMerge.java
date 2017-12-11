import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;

/**
 * Merge json objects whey they have the same key
 */
public class JsonReduce implements org.apache.flink.api.common.functions.ReduceFunction<JsonNode> {
    @Override
    public JsonNode reduce(JsonNode value1, JsonNode value2) throws Exception {
        if(value2 == null){
            return value1;
        }

        deepMerge(value1, value2);

        return value1;
    }


    /**
     * Deep merge two json objects
     */

    public JsonNode deepMerge(JsonNode mainNode, JsonNode updateNode) {
        Iterator<String> fieldNames = updateNode.fieldNames();

        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode jsonNode = mainNode.get(fieldName);

            // if field exists and is an embedded object
            if (jsonNode != null && jsonNode.isObject()) {
                deepMerge(jsonNode, updateNode.get(fieldName));
            } else {
                if (mainNode instanceof ObjectNode) {
                    // Overwrite field
                    JsonNode value = updateNode.get(fieldName);
                    ((ObjectNode) mainNode).set(fieldName, value);
                }
            }
        }

        return mainNode;
    }
}
