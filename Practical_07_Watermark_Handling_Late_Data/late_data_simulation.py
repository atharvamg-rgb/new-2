"""
Practical 7: Event-Time Watermarking and Side Output Simulator
Demonstrates exact logic of Flink's TumblingEventTimeWindows (10s) with BoundedOutOfOrderness (2s)
and routes late events into a dedicated Side Output stream.
"""

class WatermarkSimulator:
    def __init__(self, window_size_ms=10000, allowed_lateness_ms=2000):
        self.window_size = window_size_ms
        self.allowed_lateness = allowed_lateness_ms
        self.current_watermark = 0
        self.windows = {}  # window_end -> list of events
        self.closed_windows = set()

    def process_element(self, event):
        name, timestamp = event
        main_outputs = []
        side_outputs = []

        # Advance watermark based on observed maximum event time - out_of_orderness
        potential_watermark = timestamp - self.allowed_lateness
        if potential_watermark > self.current_watermark:
            self.current_watermark = potential_watermark

            # Check if any window is now past the watermark and should close
            to_close = [w_end for w_end in self.windows if w_end <= self.current_watermark and w_end not in self.closed_windows]
            for w_end in sorted(to_close):
                window_events = self.windows[w_end]
                combined_name = " + ".join(e[0] for e in window_events)
                max_ts = max(e[1] for e in window_events)
                main_outputs.append((combined_name, max_ts))
                self.closed_windows.add(w_end)

        # Calculate target window: e.g. [0, 10000) -> end=10000, [20000, 30000) -> end=30000
        window_end = ((timestamp // self.window_size) + 1) * self.window_size

        if window_end in self.closed_windows or timestamp < self.current_watermark:
            # Event arrived after window was already closed and evaluated -> SIDE OUTPUT
            side_outputs.append((name, timestamp))
        else:
            # Event belongs to active open window -> MAIN STREAM
            if window_end not in self.windows:
                self.windows[window_end] = []
            self.windows[window_end].append((name, timestamp))

        return main_outputs, side_outputs

    def flush(self):
        # Flush remaining open windows
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
    print("=" * 65)
    print("Practical 7: Watermark Handling & Side Output Simulation")
    print("Window Size: 10 seconds | Bounded Out-of-Orderness: 2 seconds")
    print("=" * 65)

    events = [
        ("Picked up", 1000),
        ("At restaurant", 8000),
        ("Delivered", 22000),      # Advances event time to 22s -> watermark reaches 20s!
        ("Order confirmed", 3000)   # Arrives late with ts=3000, but window [0, 10s) closed!
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
