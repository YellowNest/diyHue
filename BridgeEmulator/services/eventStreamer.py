import logManager
from flask import (
    Response,
    stream_with_context,
    Blueprint,
    request,
)
import json
from time import sleep, time
import HueObjects

logging = logManager.logger.get_logger(__name__)
stream = Blueprint("stream", __name__)


def messageBroker():
    # Event history is maintained in HueObjects.
    while True:
        sleep(60)


@stream.route("/eventstream/clip/v2")
def streamV2Events():

    def generate():
        last_event_id = request.headers.get(
            "Last-Event-ID"
        )

        if last_event_id:
            try:
                # Our Hue-compatible event IDs are:
                #
                #   unix_timestamp:internal_sequence
                #
                # Recover the internal sequence after reconnect.
                last_seq = int(
                    last_event_id.rsplit(":", 1)[1]
                )
            except (
                TypeError,
                ValueError,
                IndexError,
            ):
                last_seq = (
                    HueObjects.EventStreamSequence()
                )
        else:
            last_seq = (
                HueObjects.EventStreamSequence()
            )

        last_heartbeat = time()

        # Same initial SSE comment used by Hue/diyHue.
        yield ": hi\n\n"

        while True:
            events = HueObjects.EventStreamSnapshot(
                last_seq
            )

            if events:
                # Hue Bridge coalesces changes instead of sending
                # every small state update as an individual SSE
                # response.
                #
                # One SSE data payload may therefore contain several
                # Hue event containers.
                payload = [
                    message
                    for seq, message in events
                ]

                latest_seq = events[-1][0]

                # Real Hue event IDs use timestamp:index form.
                event_id = "{}:{}".format(
                    int(time()),
                    latest_seq
                )

                yield (
                    "id: {}\n"
                    "data: {}\n\n"
                ).format(
                    event_id,
                    json.dumps(
                        payload,
                        separators=(",", ":")
                    )
                )

                last_seq = latest_seq
                last_heartbeat = time()

            elif time() - last_heartbeat >= 15:
                # SSE comments are ignored by the client but keep
                # the TLS connection alive.
                yield ": keepalive\n\n"
                last_heartbeat = time()

            # Real Hue Bridge coalesces event notifications rather
            # than pushing every individual property change at
            # diyHue's previous 100 ms rate.
            sleep(1.0)

    return Response(
        stream_with_context(generate()),
        content_type=(
            "text/event-stream; charset=utf-8"
        ),
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
