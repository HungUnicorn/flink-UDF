# flink-UDF

#### Ordering trigger
A trigger for collecting all events for ordering. Instead having a window for fixed time length, the window is controlled by the trigger, and it periodically checks watermark, biggest timestamp in the window via OnEventTime to ensure() events are in ordered before reaching watermark, and fire and purge immediately.

#### ES sink
upsert/update script sink which substitues joining data on the fly because joining happens in Elasticsearch

#### Windows by element's event time
A window assigner creates dynamic window size depends on the time in the element, e.g. some elements are group into 1-minute window, some elements are group into 1-hour window
