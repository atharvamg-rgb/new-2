"""
Practical 7: PyFlink Watermark Handling and Late Data Routing to Side Output
"""
import time
from pyflink.common import Types, WatermarkStrategy, Duration, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_buffer_timeout(0)

    # Emit watermarks every 100 ms
    env.get_config().set_auto_watermark_interval(100)

    conf = Configuration()
    conf.set_string("python.fn-execution.bundle.size", "1")
    conf.set_string("python.fn-execution.bundle.time", "1")
    conf.set_string("python.operator-chaining.enabled", "false")
    env.configure(conf)

    # Delivery events: ("Order confirmed", 3000) arrives late after ("Delivered", 22000)
    events = [
        ("Picked up", 1000),
        ("At restaurant", 8000),
        ("Delivered", 22000),
        ("Order confirmed", 3000)
    ]

    data = env.from_collection(
        events,
        type_info=Types.TUPLE([Types.STRING(), Types.LONG()])
    )

    # Allow events to arrive up to 2 seconds out of order
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(2)
    ).with_timestamp_assigner(EventTimeAssigner())

    def slow(e):
        time.sleep(1)
        return e

    data = data.map(slow, output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
    stream = data.assign_timestamps_and_watermarks(watermarks)

    # Side output tag for late events
    late_tag = OutputTag(
        "late_events",
        Types.TUPLE([Types.STRING(), Types.LONG()])
    )

    # Create 10-second tumbling event-time windows
    windowed = (
        stream
        .key_by(lambda event: "delivery", key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .side_output_late_data(late_tag)
    )

    # Combine events inside main window
    main = windowed.reduce(
        lambda a, b: (a[0] + " + " + b[0], max(a[1], b[1])),
        output_type=Types.TUPLE([Types.STRING(), Types.LONG()])
    )

    # Extract late events from side output
    late = main.get_side_output(late_tag)

    main.map(
        lambda e: "MAIN OUTPUT (window result): " + str(e),
        output_type=Types.STRING()
    ).print()

    late.map(
        lambda e: "SIDE OUTPUT (late event): " + str(e),
        output_type=Types.STRING()
    ).print()

    env.execute("Food Delivery Late Data (Demo)")


if __name__ == "__main__":
    run()
