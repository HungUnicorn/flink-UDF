import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;

/**
 * This trigger ensures window collects all events it should collect and ready for ordering. 
 * It periodically checks events are in order via ctx.registerTimer(timestamp) and onEventTime() and watermark, and
 * if events are in order, the trigger is fired and purged

 * 1. Watermark condition: watermark should be bigger than the biggest timestamp(lastTimestamp) in the window
 * 2. ctx.registerTimer(timestamp): if watermark is not greater than lastTimestamp, it means events are ready for ordering.
   so it register the timestamp at lastTimestamp
   3. onEventTime() is called periodically to check events are ready for ordering
 */
public class OrderingTrigger<W extends Window> extends Trigger<JSONObject, W> {
    public static transient DateTimeFormatter timeFormatter =
          DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private long registerTimeInterval = 60000; // 1 minute

    private final ValueStateDescriptor<Long> lastTimestampState = new ValueStateDescriptor<>("lastTimestampDesc", LongSerializer.INSTANCE, 0L);

    @Override
    public TriggerResult onElement(JSONObject element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ValueState<Long> lastTimestamp = ctx.getPartitionedState(lastTimestampState);
        ctx.registerEventTimeTimer(window.maxTimestamp());

        long timestamp = DateTime.parse(element.get("timestamp").toString(), timeFormatter).getMillis();

        if (timestamp > lastTimestamp.value()) {
            lastTimestamp.update(timestamp);

        }

        if (ctx.getCurrentWatermark() >= lastTimestamp.value()) {
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            ctx.registerEventTimeTimer(lastTimestamp.value());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {

        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        ValueState<Long> lastTimestamp = ctx.getPartitionedState(lastTimestampState);

        if (ctx.getCurrentWatermark() >= lastTimestamp.value()) {
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            ctx.registerEventTimeTimer(time + registerTimeInterval);
        }
        return TriggerResult.CONTINUE;
    }

    // clears all states when the window is purged
    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(lastTimestampState).clear();
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
