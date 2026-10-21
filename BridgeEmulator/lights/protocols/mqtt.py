import logManager
import atexit
import json
import random
import threading
from time import monotonic

import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt

from functions.colors import hsv_to_rgb, convert_xy

logging = logManager.logger.get_logger(__name__)

_effect_lock = threading.Lock()
_effect_stop = {}
_effect_base_bri = {}
_effect_recent_frames = {}
_identify_lock = threading.Lock()
_identify_stop = {}
# A candidate selection can issue several identify calls in quick succession.
# Keep the *first* physical state until the final pulse completes: Z2M reports
# our temporary ON/254 frame back to diyHue, so re-reading ``light.state`` for
# every call would otherwise make a lamp that started OFF restore as ON.
_identify_restore = {}
# Hue can emit several identical V2 identify requests while the same
# candidate-selection view remains open. This timestamp is updated for every
# request so suppression remains active while the app continues refreshing.
_identify_last_seen = {}
_IDENTIFY_DUPLICATE_WINDOW = 4.0


def _mqtt_auth(mqtt_cfg):
    if (
        mqtt_cfg["mqttUser"] != ""
        and mqtt_cfg["mqttPassword"] != ""
    ):
        return {
            "username": mqtt_cfg["mqttUser"],
            "password": mqtt_cfg["mqttPassword"],
        }
    return None


# One publisher per broker configuration, local to this bridge process.  Keep
# the subscriber in services.mqtt independent: publishing from its callbacks
# must never wait for that same client's network loop.
_publishers_lock = threading.RLock()
_publishers = {}
_publishers_closed = False
_publishers_stopping = False
_MQTT_PUBLISH_TIMEOUT = 5.0


class _MqttPublisher:
    def __init__(self, mqtt_cfg):
        self.cfg = dict(mqtt_cfg)
        self.lock = threading.RLock()
        self.client = None
        self.connected = threading.Event()
        self.closed = False

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.connected.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected.clear()

    def _discard_client(self):
        client, self.client = self.client, None
        self.connected.clear()
        if client is not None:
            try:
                client.disconnect()
            finally:
                client.loop_stop()

    def _connect(self, deadline):
        if self.closed:
            raise RuntimeError("MQTT publisher is shut down")
        if self.client is not None and self.client.is_connected():
            return
        self._discard_client()
        # Reconnect on the next command using a fresh client.  Never retain or
        # replay an offline command queue (in particular an old ON after OFF).
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            reconnect_on_failure=False,
        )
        auth = _mqtt_auth(self.cfg)
        if auth is not None:
            client.username_pw_set(auth["username"], auth["password"])
        if self.cfg.get("mqttTls"):
            client.tls_set(
                ca_certs=self.cfg.get("mqttCaCerts"),
                certfile=self.cfg.get("mqttCertfile"),
                keyfile=self.cfg.get("mqttKeyfile"),
            )
            if self.cfg.get("mqttTlsInsecure"):
                client.tls_insecure_set(True)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        self.client = client
        client.connect_async(self.cfg["mqttServer"], int(self.cfg["mqttPort"]), 60)
        client.loop_start()
        if not self.connected.wait(max(0.0, deadline - monotonic())):
            raise TimeoutError("MQTT connection did not become ready")
        if not client.is_connected():
            raise ConnectionError("MQTT connection closed before publish")

    def publish(self, messages, stop_event=None):
        if not messages:
            return True
        deadline = monotonic() + _MQTT_PUBLISH_TIMEOUT
        if not self.lock.acquire(timeout=_MQTT_PUBLISH_TIMEOUT):
            raise TimeoutError("MQTT publisher busy")
        try:
            if stop_event is not None and stop_event.is_set():
                return False
            self._connect(deadline)
            # A cancelled animation must not send a frame after waiting for a
            # broker/another command.  set_light holds this same lock through
            # effect cancellation and publication of the replacement command.
            if stop_event is not None and stop_event.is_set():
                return False
            results = []
            for message in messages:
                info = self.client.publish(**message)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise ConnectionError("MQTT publish failed rc=%s" % info.rc)
                results.append(info)
            # Preserve the old helper's completion semantics. QoS 0 completion
            # means socket transmission, not broker ACK or physical response.
            # Submit a whole ordered batch before waiting; no command merging.
            for info in results:
                info.wait_for_publish(timeout=max(0.0, deadline - monotonic()))
                if not info.is_published():
                    raise TimeoutError("MQTT publish did not complete")
            return True
        except Exception:
            # Outcome may be uncertain; do not retry accepted messages. A new
            # call reconnects cleanly and cannot replay this client's queue.
            self._discard_client()
            raise
        finally:
            self.lock.release()

    def close(self):
        with self.lock:
            self.closed = True
            self._discard_client()


def _mqtt_config_key(mqtt_cfg):
    # Never log this authentication/TLS key.
    return tuple(mqtt_cfg.get(name) for name in (
        "mqttServer", "mqttPort", "mqttUser", "mqttPassword", "mqttTls",
        "mqttCaCerts", "mqttCertfile", "mqttKeyfile", "mqttTlsInsecure",
    ))


def _get_mqtt_publisher(mqtt_cfg):
    key = _mqtt_config_key(mqtt_cfg)
    with _publishers_lock:
        if _publishers_closed:
            raise RuntimeError("MQTT publishers are shut down")
        if key not in _publishers:
            _publishers[key] = _MqttPublisher(mqtt_cfg)
        return _publishers[key]


def _publish_mqtt(mqtt_cfg, messages, stop_event=None):
    return _get_mqtt_publisher(mqtt_cfg).publish(messages, stop_event)


def shutdown_mqtt_publishers():
    global _publishers_closed, _publishers_stopping
    with _publishers_lock:
        if _publishers_closed:
            return
        _publishers_stopping = True
    # Stop producers first; identify's finally block restores the captured
    # state while its publisher is still available.
    with _effect_lock:
        for event in _effect_stop.values():
            event.set()
    with _identify_lock:
        for event in _identify_stop.values():
            event.set()
    deadline = monotonic() + _MQTT_PUBLISH_TIMEOUT
    for worker in threading.enumerate():
        if worker.name.startswith("diyHue-identify-") and worker is not threading.current_thread():
            worker.join(timeout=max(0.0, deadline - monotonic()))
    with _publishers_lock:
        _publishers_closed = True
        publishers = list(_publishers.values())
    for publisher in publishers:
        publisher.close()


atexit.register(shutdown_mqtt_publishers)


def _stop_emulated_effect(topic):
    with _effect_lock:
        stop_event = _effect_stop.pop(topic, None)
        base_bri = _effect_base_bri.pop(topic, None)
        _effect_recent_frames.pop(topic, None)

    if stop_event is not None:
        stop_event.set()

    return base_bri


def _is_emulated_effect_feedback(topic, data):
    """
    Detect Z2M reports caused by our own Candle/Fire frames.

    Keep several recent frames because Zigbee/Z2M acknowledgements
    can arrive after a newer animation frame has already been sent.
    """
    now = monotonic()

    try:
        reported_bri = int(data.get("brightness"))
    except (TypeError, ValueError):
        return False

    with _effect_lock:
        if topic not in _effect_stop:
            return False

        frames = _effect_recent_frames.get(topic, [])

        # Only retain very recent animation commands.
        frames = [
            (bri, sent_at)
            for bri, sent_at in frames
            if now - sent_at <= 4.0
        ]

        matched = any(
            bri == reported_bri
            for bri, sent_at in frames
        )

        _effect_recent_frames[topic] = frames

    return (
        matched
        and data.get("state") == "ON"
    )



def _effect_timing_scale(speed):
    """
    Hue exposes effect speed as 0.0..1.0.
    Preserve our current timing at the Hue default-ish 0.5,
    make 0 slower and 1 faster without turning flicker into strobe.
    """
    try:
        speed = max(0.0, min(1.0, float(speed)))
    except (TypeError, ValueError):
        speed = 0.5

    # 0.0 -> 2.0x timing
    # 0.5 -> 1.0x timing
    # 1.0 -> 0.5x timing
    return 2.0 ** (1.0 - 2.0 * speed)


def _effect_worker(topic, effect, base_bri, speed, mqtt_cfg, stop_event):
    timing_scale = _effect_timing_scale(speed)

    # Hue brightness is treated as the reference level, not as
    # a hard upper ceiling. Native effects can visually breathe
    # slightly above and below the selected level.
    level_factor = 1.0

    try:
        while not stop_event.is_set():

            with _effect_lock:
                current_base_bri = _effect_base_bri.get(
                    topic,
                    base_bri
                )

            r = random.random()

            if effect == "candle":
                # Candle:
                # mostly subtle flicker, occasionally a short,
                # clearly visible dip or small flare.
                if r < 0.13:
                    # deeper wick flicker
                    target_factor = random.uniform(0.62, 0.78)

                elif r < 0.21:
                    # little upward flare
                    target_factor = random.uniform(1.01, 1.06)

                else:
                    # normal gentle candle movement
                    target_factor = random.uniform(0.82, 1.03)

                # Only light inertia. The previous version smoothed
                # this too heavily and made the effect look static.
                level_factor = (
                    level_factor * 0.12
                    + target_factor * 0.88
                )

                transition = (
                    random.uniform(0.14, 0.30)
                    * timing_scale
                )

                # Occasionally make two movements arrive closer
                # together, like a short candle flicker.
                if random.random() < 0.20:
                    delay = (
                        random.uniform(0.32, 0.52)
                        * timing_scale
                    )
                else:
                    delay = (
                        random.uniform(0.58, 1.02)
                        * timing_scale
                    )

            elif effect == "fire":
                # Fireplace:
                # more movement and a larger brightness envelope
                # than Candle -- "dancing flames", not a slow pulse.
                if r < 0.18:
                    # noticeable flame dip
                    target_factor = random.uniform(0.54, 0.73)

                elif r < 0.34:
                    # flame-up
                    target_factor = random.uniform(1.02, 1.10)

                else:
                    # normal dancing flame
                    target_factor = random.uniform(0.70, 1.04)

                level_factor = (
                    level_factor * 0.10
                    + target_factor * 0.90
                )

                transition = (
                    random.uniform(0.14, 0.32)
                    * timing_scale
                )

                # Fireplace gets slightly more frequent short
                # movements than Candle, but stays below a rate
                # that would unnecessarily hammer Zigbee.
                if random.random() < 0.28:
                    delay = (
                        random.uniform(0.30, 0.50)
                        * timing_scale
                    )
                else:
                    delay = (
                        random.uniform(0.52, 0.92)
                        * timing_scale
                    )

            else:
                return

            brightness = max(
                1,
                min(
                    254,
                    round(current_base_bri * level_factor)
                )
            )

            payload = {
                "state": "ON",
                "brightness": brightness,
                "transition": transition,
            }

            # OFF may arrive asynchronously from Home Assistant /
            # Zigbee2MQTT. Never publish another effect frame after
            # the stop event has been raised.
            if stop_event.is_set():
                break

            # Remember this exact animation frame so the MQTT state
            # subscriber can distinguish it from a real HA/user
            # brightness change.
            with _effect_lock:
                frames = _effect_recent_frames.setdefault(
                    topic,
                    []
                )

                frames.append(
                    (int(brightness), monotonic())
                )

                # More than enough to cover normal Z2M latency
                # without allowing the list to grow indefinitely.
                if len(frames) > 16:
                    del frames[:-16]

            try:
                _publish_mqtt(
                    mqtt_cfg,
                    [{"topic": topic, "payload": json.dumps(payload)}],
                    stop_event,
                )

            except Exception as e:
                logging.warning(
                    "MQTT emulated %s effect failed for %s: %s",
                    effect,
                    topic,
                    e,
                )

            stop_event.wait(delay)

    finally:
        with _effect_lock:
            if _effect_stop.get(topic) is stop_event:
                _effect_stop.pop(topic, None)
                _effect_base_bri.pop(topic, None)
                _effect_recent_frames.pop(topic, None)



def _start_emulated_effect(topic, effect, base_bri, speed, mqtt_cfg):
    if _publishers_stopping:
        return
    _stop_emulated_effect(topic)

    stop_event = threading.Event()

    with _effect_lock:
        _effect_stop[topic] = stop_event
        _effect_base_bri[topic] = base_bri

    threading.Thread(
        target=_effect_worker,
        args=(topic, effect, base_bri, speed, mqtt_cfg, stop_event),
        name="diyHue-%s-%s" % (effect, topic),
        daemon=True,
    ).start()


def _stop_mqtt_identify(topic):
    with _identify_lock:
        # Leave the event registered so its worker owns the restore step in
        # ``finally``. Removing it here could strand a lamp on its temporary
        # identify brightness.
        stop_event = _identify_stop.get(topic)
    if stop_event is not None:
        stop_event.set()


def _start_mqtt_identify(light):
    """Blink a Zigbee2MQTT lamp and restore its prior physical state.

    Several Zigbee lamps advertise ZCL ``blink`` but ignore that native
    effect while off. MotionAware candidate selection must still be visible,
    so use a short ON/OFF/ON pulse and restore the state captured at the
    beginning. This is intentionally protocol-local and never changes diyHue
    resource state directly.
    """
    if _publishers_stopping:
        return False
    topic = light.protocol_cfg.get("command_topic")
    mqtt_cfg = light.protocol_cfg.get("mqtt_server")
    if not topic or not mqtt_cfg:
        return False

    stop_event = threading.Event()
    with _identify_lock:
        now = monotonic()
        previous_seen = _identify_last_seen.get(topic)
        if (
            previous_seen is not None
            and now - previous_seen < _IDENTIFY_DUPLICATE_WINDOW
        ):
            # The V2 identify request has no selection/deselection indicator.
            # Refresh the quiet period. A fixed window from the first request
            # would otherwise make the lamp flash at a fixed interval while
            # the app continues sending the same identify request.
            _identify_last_seen[topic] = now
            logging.info("MQTT identify duplicate suppressed for %s", topic)
            return True

        previous_stop = _identify_stop.get(topic)
        restore = _identify_restore.get(topic)

        if restore is None:
            try:
                brightness = max(
                    1,
                    min(254, int(light.state.get("bri", 254)))
                )
            except (TypeError, ValueError):
                brightness = 254
            restore = {
                "on": bool(light.state.get("on", False)),
                "brightness": brightness,
            }

        _identify_stop[topic] = stop_event
        _identify_restore[topic] = restore
        _identify_last_seen[topic] = now

    # Do this after publishing the replacement worker as the active one.  A
    # cancelled worker may still enter ``finally``; it must not restore over
    # the new worker's pulse sequence.
    if previous_stop is not None:
        previous_stop.set()

    def publish_step(payload, cancelled=None):
        return _publish_mqtt(
            mqtt_cfg,
            [{"topic": topic, "payload": json.dumps(payload)}],
            cancelled,
        )

    def identify_worker():
        try:
            # Two short bright pulses remain visible even when the selected
            # lamp was off at minimum brightness. Z2M effects alone are not
            # reliable for this case on otherwise compatible IKEA hardware.
            for payload, duration in (
                ({"state": "ON", "brightness": 254, "transition": 0.1}, 0.30),
                ({"state": "OFF", "transition": 0.1}, 0.20),
                ({"state": "ON", "brightness": 254, "transition": 0.1}, 0.30),
            ):
                if stop_event.is_set():
                    return
                if not publish_step(payload, stop_event):
                    return
                if stop_event.wait(duration):
                    return

        except Exception as error:
            logging.warning("MQTT identify failed for %s: %s", topic, error)
        finally:
            # Only the last worker for a topic may restore the lamp.  It owns
            # the initial physical state captured above, not an echoed pulse
            # state from Zigbee2MQTT.
            with _identify_lock:
                if _identify_stop.get(topic) is stop_event:
                    _identify_stop.pop(topic, None)
                    restore = _identify_restore.pop(topic, None)
                else:
                    restore = None

            if restore is not None:
                try:
                    publish_step({
                        "state": "ON" if restore["on"] else "OFF",
                        "brightness": restore["brightness"],
                        "transition": 0.1,
                    })
                except Exception as error:
                    logging.warning(
                        "MQTT identify restore failed for %s: %s",
                        topic,
                        error,
                    )

    threading.Thread(
        target=identify_worker,
        name="diyHue-identify-%s" % topic,
        daemon=True,
    ).start()
    return True


def identify_light(light):
    return _start_mqtt_identify(light)


def set_light(light, data):
    if _publishers_stopping:
        raise RuntimeError("MQTT publishers are shutting down")
    publisher = _get_mqtt_publisher(light.protocol_cfg["mqtt_server"])
    if not publisher.lock.acquire(timeout=_MQTT_PUBLISH_TIMEOUT):
        raise TimeoutError("MQTT publisher busy")
    try:
        return _set_light(light, data)
    finally:
        publisher.lock.release()


def _set_light(light, data):
    messages = []
    lightsData = {}

    if "lights" not in data:
        lightsData = {
            light.protocol_cfg["command_topic"]: data
        }
    else:
        lightsData = data["lights"]

    mqtt_cfg = light.protocol_cfg["mqtt_server"]

    effects_to_start = []

    for topic in lightsData.keys():
        command = lightsData[topic]
        payload = {"transition": 0.3}
        colorFromHsv = False

        effect = command.get("effect")

        try:
            effect_speed = max(
                0.0,
                min(
                    1.0,
                    float(command.get("effect_speed", 0.5))
                )
            )
        except (TypeError, ValueError):
            effect_speed = 0.5

        emulate_z2m = topic.startswith("zigbee2mqtt/")

        old_base_bri = None

        if effect in ("candle", "fire") and emulate_z2m:
            old_base_bri = _stop_emulated_effect(topic)

        elif effect == "no_effect" and emulate_z2m:
            old_base_bri = _stop_emulated_effect(topic)

        elif effect is not None:
            old_base_bri = _stop_emulated_effect(topic)

        # OFF really stops the running effect.
        elif command.get("on") is False:
            _stop_emulated_effect(topic)

        # Colour/CT changes do NOT stop a running Hue effect.
        # Native Hue allows changing the effect colour while
        # Candle/Fire remains active. The payload below will update
        # the physical colour while the effect worker keeps running.

        # Hue can send brightness updates while an effect remains
        # active. Update the effect level instead of stopping it.
        elif "bri" in command:
            try:
                new_base_bri = max(
                    1,
                    min(254, int(command["bri"]))
                )
            except (TypeError, ValueError):
                new_base_bri = None

            if new_base_bri is not None:
                with _effect_lock:
                    if topic in _effect_base_bri:
                        _effect_base_bri[topic] = new_base_bri

        # Deliberately do nothing for on=True.
        # Hue sends harmless ON refreshes while effects are active.

        for key, value in command.items():

            if key == "on":
                payload["state"] = "ON" if value is True else "OFF"

            if key == "bri":
                payload["brightness"] = value

            if key == "xy":
                payload["color"] = {
                    "x": value[0],
                    "y": value[1],
                }

            if key == "gradient":
                rgbs = list(
                    map(
                        lambda xy_record: convert_xy(
                            xy_record["color"]["xy"]["x"],
                            xy_record["color"]["xy"]["y"],
                            255,
                        ),
                        value["points"],
                    )
                )

                hexes = list(
                    map(
                        lambda rgb:
                            "#"
                            + format(int(round(rgb[0])), "02x")
                            + format(int(round(rgb[1])), "02x")
                            + format(int(round(rgb[2])), "02x"),
                        rgbs,
                    )
                )

                hexes.reverse()
                payload["gradient"] = hexes

            if key == "ct":
                payload["color_temp"] = value

            if key == "hue" or key == "sat":
                colorFromHsv = True

            if key == "alert" and value != "none":
                payload["alert"] = value

            if key == "transitiontime":
                payload["transition"] = value / 10

            if key == "effect":

                if emulate_z2m and value in ("candle", "fire"):
                    pass

                elif emulate_z2m and value == "no_effect":
                    # candle/fire are emulated locally by diyHue.
                    # Stopping them must therefore remain local too.
                    #
                    # Do NOT translate Hue "no_effect" to a made-up
                    # Zigbee2MQTT effect such as "stop_colorloop".
                    # Many Z2M lights reject that value entirely.
                    pass

                elif emulate_z2m and value == "colorloop":
                    # Legacy Hue colorloop is not a Zigbee2MQTT effect
                    # capability. Never forward the Hue API value raw
                    # to the physical Zigbee device.
                    logging.info(
                        "MQTT suppressing unsupported raw Hue "
                        "colorloop command for %s",
                        topic,
                    )
                    pass

                else:
                    payload["effect"] = value

        if colorFromHsv:
            hue = command.get(
                "hue",
                light.state.get("hue", 0)
            )

            sat = command.get(
                "sat",
                light.state.get("sat", 0)
            )

            bri = command.get(
                "bri",
                light.state.get("bri", 254)
            )

            color = hsv_to_rgb(hue, sat, bri)

            payload["color"] = {
                "r": color[0],
                "g": color[1],
                "b": color[2],
            }

        if effect == "no_effect" and old_base_bri is not None:
            if "brightness" not in payload:
                payload["brightness"] = old_base_bri

        if len(payload) > 1:
            messages.append({
                "topic": topic,
                "payload": json.dumps(payload),
            })

        if effect in ("candle", "fire") and emulate_z2m:
            base_bri = command.get(
                "bri",
                light.state.get("bri", 160)
            )

            if base_bri is None:
                base_bri = 160

            base_bri = max(
                1,
                min(254, int(base_bri))
            )

            effects_to_start.append(
                (topic, effect, base_bri, effect_speed)
            )

    logging.debug(
        "MQTT publish to: " + json.dumps(messages)
    )

    if messages:
        _publish_mqtt(mqtt_cfg, messages)

    for topic, effect, base_bri, effect_speed in effects_to_start:
        logging.info(
            "Starting emulated Hue %s effect for %s at speed %.2f",
            effect,
            topic,
            effect_speed,
        )

        _start_emulated_effect(
            topic,
            effect,
            base_bri,
            effect_speed,
            mqtt_cfg,
        )


def get_light_state(light):
    return {}


def discover(mqtt_config):
    if mqtt_config["enabled"]:
        logging.info("MQTT discovery called")

        auth = _mqtt_auth(mqtt_config)

        try:
            publish.single(
                "zigbee2mqtt/bridge/request/permit_join",
                json.dumps({
                    "value": True,
                    "time": 120,
                }),
                hostname=mqtt_config["mqttServer"],
                port=mqtt_config["mqttPort"],
                auth=auth,
            )

            publish.single(
                "zigbee2mqtt/bridge/config/devices/get",
                hostname=mqtt_config["mqttServer"],
                port=mqtt_config["mqttPort"],
                auth=auth,
            )

        except Exception as e:
            logging.error(
                "MQTT publish failed: " + str(e)
            )
