import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The time of this window assigner is given by event time in elements
 */
public class ElementSlidingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long slide;

    protected ElementSlidingEventTimeWindows(long slide) {
        this.slide = slide;
    }

    public long getSlide() {
        return slide;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            // The windows know how long they should be created
            long size = (long) ((JSONObject) element).get("interval");
            List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
            long lastStart = timestamp - timestamp % slide;
            for (long start = lastStart;
                 start > timestamp - size;
                 start -= slide) {
                windows.add(new TimeWindow(start, start + size));
            }
            return windows;
        } else {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
                    "Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
                    "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "SlidingEventTimeWindows(" + slide + ")";
    }

    /**
     * Creates a new {@code SlidingEventTimeWindows} {@link WindowAssigner} that assigns
     * elements to sliding time windows based on the element timestamp.
     *
     * @param slide The slide interval of the generated windows.
     * @return The time policy.
     */
    public static ElementSlidingEventTimeWindows of(Time slide) {
        return new ElementSlidingEventTimeWindows(slide.toMilliseconds());
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
