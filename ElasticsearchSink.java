import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequest;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * Shared Elasticsearch configuration for production, which contains
 *
 * 1. Automatic discovering new nodes (client.transport.sniff)
 * 2. Exponential retry (backoff)
 * 3. Bulk insert with size and time limitation (bulk.flush)
 * 4. Handle request failure
 *
 * Usage:
 * Extends this class in your Sink class
 *
 */
public abstract class SharedElasticsearchSink{


    private static final Logger log = Logger.getLogger(SharedElasticsearchSink.class.getName());
    public static Map<String, String> config = Maps.newHashMap();
    public static List<InetSocketAddress> transports = new ArrayList<>();
    public static ActionRequestFailureHandlerImpl actionRequestFailureHandlerImpl;

    public SharedElasticsearchSink() {
        String clientTransportSniff = System.getenv("CLIENT_TRANSPORT_SNIFF");
        if (clientTransportSniff == null || clientTransportSniff.isEmpty()) {
            clientTransportSniff = "true";
        }
        config.put("client.transport.sniff", clientTransportSniff);
        config.put("cluster.name", System.getenv("ELASTICSEARCH_CLUSTER_NAME"));
        config.put("bulk.flush.max.size.mb", "1");
        config.put("bulk.flush.interval.ms", "5000");
        config.put("bulk.flush.backoff.enable", "true");
        config.put("bulk.flush.backoff.type", "EXPONENTIAL");
        // Retry more than master re-selection
        config.put("bulk.flush.backoff.delay",
                Optional.ofNullable(System.getenv("ELASTICSEARCH_BACKOFF_DELAY"))
                        .orElse("2000"));
        config.put("bulk.flush.backoff.retries",
                Optional.ofNullable(System.getenv("ELASTICSEARCH_BACKOFF_RETRIES"))
                        .orElse("100"));

        transports.add(new InetSocketAddress(System.getenv("ELASTICSEARCH_HOST"), 9300));

        actionRequestFailureHandlerImpl = new ActionRequestFailureHandlerImpl();

    }

    public class ActionRequestFailureHandlerImpl implements ActionRequestFailureHandler {
        @Override
        public void onFailure(ActionRequest actionRequest, Throwable throwable, int statusCode,
                              RequestIndexer requestIndexer)
                throws Throwable {
            Iterator<ObjectObjectCursor<Object, Object>> it = actionRequest.getContext().iterator();
            while (it.hasNext()) {
                ObjectObjectCursor<Object, Object> current = it.next();
                log.error("Fail request after exceeding retries, with request: " + current.toString() +
                        ", exception: " + throwable.toString() + ", statusCode: " + statusCode);
            }
        }
    }
}
