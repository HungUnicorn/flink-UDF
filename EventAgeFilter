import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To restrict multiple event types with the same event ages when processing
 */
public class EventAgeFilter implements FilterFunction<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(EventAgeFilter.class.getName());

    public final static transient DateTimeFormatter parseFromTimeFormatter = ISODateTimeFormat.dateTimeParser();
    private final timestampsField = "rowtime";
    
    private DateTime maxEventAgeTimestamp;

    public EventAgeFilter(DateTime timestamp){
        maxEventAgeTimestamp = timestamp;
    }

    @Override
    public boolean filter(JsonNode jsonObject) throws Exception {
        DateTime occurredAtDateTime;

        try {
            occurredAtDateTime = parseFromTimeFormatter.parseDateTime(jsonObject.get(Config.METADATA)
                    .get(timestampsField).asText());

        }
        catch(IllegalArgumentException ie){
            LOG.error("Parse error for event: " +  jsonObject.toString());
            return false;
        }

        return occurredAtDateTime.compareTo(maxEventAgeTimestamp) == 1;
    }
}
