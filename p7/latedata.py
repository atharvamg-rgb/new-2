"""
Practical 7: PyFlink Watermark Handling and Late Data Routing to Side Output
Uses KeyedProcessFunction with Bounded Out-of-Orderness Watermarking and Side Outputs.
"""
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.functions import KeyedProcessFunction


class LateDataProcessFunction(KeyedProcessFunction):
    """
    Simulates Event-Time Windowing with Watermarks and Side Outputs:
    - Window Size: 10,000 ms (10 seconds)
    - Bounded Out-of-Orderness: 2,000 ms (2 seconds)
    - Late events (> watermark) are routed to side output.
    """
    def __init__(self, window_size=10000, out_of_orderness=2000):
        self.window_size = window_size
        self.out_of_orderness = out_of_orderness
        self.late_tag = OutputTag("late_events", Types.TUPLE([Types.STRING(), Types.LONG()]))
        self.current_watermark = 0
        self.windows = {}
        self.closed_windows = set()

    def process_element(self, value, ctx):
        name, ts = value

        # Monotonically advance watermark = max(event_time) - bounded_out_of_orderness
        potential_wm = ts - self.out_of_orderness
        if potential_wm > self.current_watermark:
            self.current_watermark = potential_wm

            # Close and fire all windows whose end time <= current watermark
            to_close = [
                w_end for w_end in self.windows
                if w_end <= self.current_watermark and w_end not in self.closed_windows
            ]
            for w_end in sorted(to_close):
                events = self.windows[w_end]
                combined = " + ".join(e[0] for e in events)
                max_ts = max(e[1] for e in events)
                yield (combined, max_ts)
                self.closed_windows.add(w_end)

        # Skip internal flush marker
        if name == "__END__":
            return

        window_end = ((ts // self.window_size) + 1) * self.window_size

        # If target window already closed or timestamp < watermark, route to SIDE OUTPUT
        if window_end in self.closed_windows or ts < self.current_watermark:
            yield self.late_tag, (name, ts)
        else:
            if window_end not in self.windows:
                self.windows[window_end] = []
            self.windows[window_end].append((name, ts))


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Simulated Delivery Stream:
    # ("Order confirmed", 3000) arrives late after ("Delivered", 22000)
    events = [
        ("Picked up", 1000),
        ("At restaurant", 8000),
        ("Delivered", 22000),
        ("Order confirmed", 3000),
        ("__END__", 99999)  # Triggers final window closure
    ]

    type_info = Types.TUPLE([Types.STRING(), Types.LONG()])
    ds = env.from_collection(events, type_info=type_info)

    proc = LateDataProcessFunction(window_size=10000, out_of_orderness=2000)

    # Key by stream key and process with side output
    main_stream = (
        ds.key_by(lambda x: "delivery", key_type=Types.STRING())
        .process(proc, output_type=type_info)
    )

    # Extract side output stream for late arriving events
    late_stream = main_stream.get_side_output(proc.late_tag)

    # Print Main and Side Outputs
    main_stream.map(
        lambda e: "MAIN OUTPUT (window result): " + str(e),
        output_type=Types.STRING()
    ).print()

    late_stream.map(
        lambda e: "SIDE OUTPUT (late event):   " + str(e),
        output_type=Types.STRING()
    ).print()

    env.execute("Food Delivery Late Data Processing")


if __name__ == "__main__":
    run()
