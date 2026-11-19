# Practical 7: Watermark Handling & Late Data Routing with Side Outputs

## Aim
To create a stream processor that correctly handles **Late Arriving Data** in out-of-order streams using **Watermarks** and **Side Outputs**.

---

## Theory & Concept

### Event Time vs. Processing Time
- **Processing Time:** The system clock time of the machine executing the operator.
- **Event Time:** The timestamp embedded within each event when it occurred at the source (e.g. mobile client or IoT sensor).

### Watermarks & Bounded Out-of-Orderness
In real-world networks, packets arrive out of order due to network latency. A **Watermark** $W(t)$ is a monotonically increasing timestamp declaring that no further events with timestamp $\le W(t)$ are expected.
$$\text{Watermark} = \max(\text{Event Time}) - \text{Bounded Out-of-Orderness}$$

### Side Outputs
When an event arrives after its target window has already closed and fired (e.g. `Order confirmed` at $t=3000\text{ ms}$ arriving after `Delivered` at $t=22000\text{ ms}$ has already pushed the watermark past $20000\text{ ms}$), discarding it causes data loss. **Side Outputs** safely divert late events into a dedicated secondary stream for reprocessing or anomaly logging.

---

## Execution Instructions

### Running the Simulator:
```powershell
cd Practical_07_Watermark_Handling_Late_Data
python late_data_simulation.py
```

### Running with Apache PyFlink (if installed):
```powershell
python latedata.py
```

---

## Complete Source Codes

### 1. `late_data_simulation.py`
```python
class WatermarkSimulator:
    def __init__(self, window_size_ms=10000, allowed_lateness_ms=2000):
        self.window_size = window_size_ms
        self.allowed_lateness = allowed_lateness_ms
        self.current_watermark = 0
        self.windows = {}
        self.closed_windows = set()

    def process_element(self, event):
        name, timestamp = event
        main_outputs = []
        side_outputs = []

        potential_watermark = timestamp - self.allowed_lateness
        if potential_watermark > self.current_watermark:
            self.current_watermark = potential_watermark

            to_close = [w_end for w_end in self.windows if w_end <= self.current_watermark and w_end not in self.closed_windows]
            for w_end in sorted(to_close):
                window_events = self.windows[w_end]
                combined_name = " + ".join(e[0] for e in window_events)
                max_ts = max(e[1] for e in window_events)
                main_outputs.append((combined_name, max_ts))
                self.closed_windows.add(w_end)

        window_end = ((timestamp // self.window_size) + 1) * self.window_size

        if window_end in self.closed_windows or timestamp < self.current_watermark:
            side_outputs.append((name, timestamp))
        else:
            if window_end not in self.windows:
                self.windows[window_end] = []
            self.windows[window_end].append((name, timestamp))

        return main_outputs, side_outputs

    def flush(self):
        main_outputs = []
        for w_end in sorted(self.windows.keys()):
            if w_end not in self.closed_windows:
                window_events = self.windows[w_end]
                combined_name = " + ".join(e[0] for e in window_events)
                max_ts = max(e[1] for e in window_events)
                main_outputs.append((combined_name, max_ts))
                self.closed_windows.add(w_end)
        return main_outputs


if __name__ == "__main__":
    events = [
        ("Picked up", 1000),
        ("At restaurant", 8000),
        ("Delivered", 22000),
        ("Order confirmed", 3000)
    ]

    sim = WatermarkSimulator(window_size_ms=10000, allowed_lateness_ms=2000)

    for event in events:
        main_res, side_res = sim.process_element(event)
        for m in main_res:
            print(f"MAIN OUTPUT (window result): {m}")
        for s in side_res:
            print(f"SIDE OUTPUT (late event):   {s}")

    for m in sim.flush():
        print(f"MAIN OUTPUT (window result): {m}")
```

### 2. `latedata.py` (PyFlink Implementation)
```python
from pyflink.common import Types, WatermarkStrategy, Duration, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

events = [
    ("Picked up", 1000),
    ("At restaurant", 8000),
    ("Delivered", 22000),
    ("Order confirmed", 3000)
]

data = env.from_collection(events, type_info=Types.TUPLE([Types.STRING(), Types.LONG()]))

watermarks = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(2)
).with_timestamp_assigner(EventTimeAssigner())

stream = data.assign_timestamps_and_watermarks(watermarks)

late_tag = OutputTag("late_events", Types.TUPLE([Types.STRING(), Types.LONG()]))

windowed = (
    stream
    .key_by(lambda event: "delivery", key_type=Types.STRING())
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .side_output_late_data(late_tag)
)

main = windowed.reduce(
    lambda a, b: (a[0] + " + " + b[0], max(a[1], b[1])),
    output_type=Types.TUPLE([Types.STRING(), Types.LONG()])
)

late = main.get_side_output(late_tag)

main.map(lambda e: "MAIN OUTPUT (window result): " + str(e), output_type=Types.STRING()).print()
late.map(lambda e: "SIDE OUTPUT (late event):   " + str(e), output_type=Types.STRING()).print()

env.execute("Food Delivery Late Data (Demo)")
```

---

## Expected Output

```
MAIN OUTPUT (window result): ('Picked up + At restaurant', 8000)
SIDE OUTPUT (late event):   ('Order confirmed', 3000)
MAIN OUTPUT (window result): ('Delivered', 22000)
```
