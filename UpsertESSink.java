import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ES upsert requests
 *
 */
public class UpserESSink implements Serializable {

    private Map<String, String> config = Maps.newHashMap();
    private String elasticsearch;

    public ESUpsertRequestEventCounts(String elasticsearch) {
        // Best bulk size: 5-15 MB
        // Ref: https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html
        config.put("bulk.flush.max.size.mb", "50");

        config.put("bulk.flush.interval.ms", "10000");
        config.put("cluster.name", "YOUR_ES_NAME");
        this.elasticsearch = elasticsearch;
    }

    // Transport client
    public ElasticsearchSink elasticsearchSink(){

        List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
        transports.add(new InetSocketAddress(this.elasticsearch, 9300));
        return new ElasticsearchSink<>(config, transports, new ElasticsearchSinkFunction<JSONObject>() {
            @Override
            public void process(JSONObject element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            }
        });
    }

    public static UpdateRequest updateRequest (JSONObject element) {
      // If doc doens't exist, upsert() is used, if doc exists, updateRequest is used
      String index = "Your_ES_INDEX"
      String type = "YOUR_ES_TYPE"
      docId = String.valueOf(element.get("id"))
        return new UpdateRequest()
                .index(index)
                .type(type)
                .id(docId)
                .doc(element)
                .upsert(Requests.indexRequest()
                        .index(index)
                        .type(type)
                        .id(docId)
                        .source(element));

    }

    // If item exists, it uses update script (append the value). If not, use index request
    // Doesn't work if upsert and update come very close because when upsert doesn't complete, elasticsearch
    // will try the second upsert
    public static UpdateRequest updateRequestScript(JSONObject element) {
        String index = "Your_ES_INDEX"
        String type = "YOUR_ES_TYPE"
        String docId = String.valueOf(element.get("id"));
        Map<String, Integer> countParam = new HashMap<>();
        countParam.put("count", Integer.parseInt(element.get("count").toString()));
        return new UpdateRequest()
                .index(index)
                .type("type")
                .id(docId)
                .script(new Script("ctx._source.count += count", ScriptService.ScriptType.INLINE, "groovy", countParam))
                .upsert(createIndexRequest(element));
    }
}
