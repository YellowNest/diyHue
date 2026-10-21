import logManager
import configManager
import json
import math
import importlib
import weakref
import ssl
import time
from HueObjects import Sensor
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from threading import Thread
from time import sleep
from functions.core import nextFreeId
from sensors.discover import addHueMotionSensor
from sensors.sensor_types import sensorTypes, MODEL_ALIASES
from lights.discover import addNewLight
from functions.rules import rulesProcessor
from functions.behavior_instance import checkBehaviorInstances
import requests
import os
import statistics
from collections import deque
from threading import Condition, Lock

logging = logManager.logger.get_logger(__name__)
bridgeConfig = configManager.bridgeConfig.yaml_config
client = mqtt.Client()

devices_ids = {}

# Configuration stuff
discoveryPrefix = "homeassistant"
latestStates = {}
discoveredDevices = {}

# Last reported Zigbee2MQTT bridge connectivity.
# None means no bridge/state message has been received yet.
mqttBridgeOnline = None

# Prevent overlapping physical-state refreshes.
mqttStateSyncRunning = False


# ===========================================================================
# Hue Bridge Pro MotionAware RF/LQI detector
# ===========================================================================

# MQTT callbacks publish LQI samples asynchronously.  The worker waits on
# this condition rather than sleeping for the complete response window when
# enough independent links have already answered.
_motionAwareRadioLock = Condition(Lock())

# topic -> (monotonic timestamp, linkquality)
_motionAwareRadioSamples = {}

# (area_id, topic) -> quiet baseline samples
_motionAwareRadioHistory = {}

# area -> latest detected motion timestamp
_motionAwareRadioLastMotion = {}

# area -> current boolean state
_motionAwareRadioState = {}

# area -> consecutive, spatially-coherent active RF links.  This is runtime
# learning only: persisted MotionAware configuration remains in motionAware.
_motionAwareRadioConfirmation = {}

_motionAwareRadioWorkerRunning = False

_MOTIONAWARE_BASELINE_SAMPLES = 12
# Zigbee2MQTT replies from the selected MotionAware participants arrive well
# inside the response window on the live bridge.  A 250 ms cadence removes
# half of the confirmation latency while retaining the full-graph, stable
# pair, and three-round false-positive guards below.
_MOTIONAWARE_POLL_INTERVAL = 0.25
_MOTIONAWARE_RESPONSE_WINDOW = 0.60
_MOTIONAWARE_HOLD_SECONDS = 4.0
# LQI is a noisy RF proxy.  Require the same pair of selected participant
# links to remain active over adjacent poll rounds before it becomes motion.
# These timing guards are topology-independent; sensitivity still controls
# the signal-amplitude threshold per selected area.
_MOTIONAWARE_CONFIRMATION_ROUNDS = 3
_MOTIONAWARE_CONFIRMATION_MAX_GAP = 1.5


def resetMotionAwareRadioArea(area_id):
    """Forget transient RF learning for a changed/disabled area.

    Configuration owns the persistent area graph; this worker owns only
    process-local samples, holds and confirmations.  Keeping the reset here
    makes participant edits and enable transitions deterministic without
    writing runtime state back to config.yaml.
    """
    if not isinstance(area_id, str) or not area_id:
        return False

    with _motionAwareRadioLock:
        history_keys = [
            key for key in _motionAwareRadioHistory
            if key[0] == area_id
        ]
        for key in history_keys:
            del _motionAwareRadioHistory[key]
        last_motion = _motionAwareRadioLastMotion.pop(
            area_id,
            None,
        )
        state = _motionAwareRadioState.pop(area_id, None)
        confirmation = _motionAwareRadioConfirmation.pop(
            area_id,
            None,
        )

    changed = (
        bool(history_keys)
        or last_motion is not None
        or state is not None
        or confirmation is not None
    )
    if changed:
        logging.info(
            "MotionAware RF baseline reset area=%s",
            area_id,
        )
    return changed


def _is_motionaware_radio_enabled():
    return (
        os.getenv(
            "BRIDGE_PROFILE",
            ""
        ).lower() == "pro"
    )


def _motionaware_radio_thresholds(sensitivity):
    # Higher Hue sensitivity -> lower RF threshold.
    table = {
        0: (14.0, 3.0),
        1: (12.0, 2.7),
        2: (10.0, 2.4),
        3: (9.0, 2.2),
        4: (8.0, 2.0),
    }

    try:
        sensitivity = int(sensitivity)
    except (TypeError, ValueError):
        sensitivity = 2

    sensitivity = max(
        0,
        min(4, sensitivity)
    )

    return table[sensitivity]


def _motionaware_robust_spread(values):
    if len(values) < 4:
        return 1.0

    center = statistics.median(values)

    mad = statistics.median(
        abs(value - center)
        for value in values
    )

    return max(
        1.0,
        1.4826 * mad
    )


def _motionaware_radio_observe(topic, data):
    if not _is_motionaware_radio_enabled():
        return

    if not isinstance(data, dict):
        return

    value = data.get("linkquality")

    if not isinstance(
        value,
        (int, float)
    ):
        return

    if not (
        isinstance(topic, str)
        and topic.startswith("zigbee2mqtt/")
    ):
        return

    with _motionAwareRadioLock:
        _motionAwareRadioSamples[topic] = (
            time.monotonic(),
            float(value),
        )
        _motionAwareRadioLock.notify_all()


def _motionaware_radio_has_complete_fresh_samples(specs, round_started):
    """Return true once every selected participant replied this round.

    A MotionAware area calibrates a selected three--four light graph.  If the
    worker wakes after only two replies, consistently-late participants can
    never receive a baseline.  We therefore wait for the full graph, bounded
    by the normal response timeout.
    """
    for spec in specs:
        fresh_count = sum(
            1
            for topic in spec["topics"]
            if (
                (sample := _motionAwareRadioSamples.get(topic)) is not None
                and sample[0] >= round_started
            )
        )
        if fresh_count < len(spec["topics"]):
            return False
    return True


def _motionaware_radio_wait_for_samples(specs, round_started):
    """Wait for complete selected-graph replies, bounded by the timeout."""
    deadline = time.monotonic() + _MOTIONAWARE_RESPONSE_WINDOW
    with _motionAwareRadioLock:
        while not _motionaware_radio_has_complete_fresh_samples(
            specs,
            round_started,
        ):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            _motionAwareRadioLock.wait(timeout=remaining)


def _motionaware_radio_area_specs():
    try:
        from functions.motionAware import (
            motionAwareStoredAreas,
            motionAwareStoredArea,
            v2MotionAwareRooms,
            v2MotionAreaIds,
            v2MotionAreaDevices,
            isMotionAwareAreaRuntimeEnabled,
        )
    except Exception:
        logging.exception(
            "MotionAware radio import failed"
        )
        return []

    stored = motionAwareStoredAreas()
    result = []

    for group, devices in v2MotionAwareRooms():
        area_id, _, _ = v2MotionAreaIds(
            group
        )

        if area_id not in stored:
            continue

        area = motionAwareStoredArea(
            area_id
        )
        devices = v2MotionAreaDevices(group, area_id)

        if not isMotionAwareAreaRuntimeEnabled(area_id):
            continue

        topics = []

        for device in devices:
            if getattr(
                device,
                "protocol",
                None
            ) != "mqtt":
                continue

            topic = getattr(
                device,
                "protocol_cfg",
                {}
            ).get(
                "state_topic"
            )

            if (
                isinstance(topic, str)
                and topic.startswith(
                    "zigbee2mqtt/"
                )
            ):
                topics.append(topic)

        topics = sorted(set(topics))

        # Current MotionAware contract needs 3-4 participants.
        if len(topics) < 3:
            continue

        sensitivities = []

        for resource_type in (
            "convenience_area_motion",
            "security_area_motion",
        ):
            cfg = area.get(
                resource_type,
                {}
            )

            if isinstance(cfg, dict):
                if cfg.get("enabled", True) is False:
                    continue
                sensitivities.append(
                    cfg.get(
                        "sensitivity",
                        2
                    )
                )

        clean_sensitivities = []

        for value in sensitivities:
            try:
                clean_sensitivities.append(
                    int(value)
                )
            except (TypeError, ValueError):
                pass

        sensitivity = (
            max(clean_sensitivities)
            if clean_sensitivities
            else 2
        )

        result.append({
            "area_id": area_id,
            "name": getattr(
                group,
                "name",
                area_id
            ),
            "topics": topics,
            "sensitivity": max(
                0,
                min(4, sensitivity)
            ),
        })

    return result


def _motionaware_radio_set_state(
    area_id,
    value
):
    value = bool(value)

    previous = _motionAwareRadioState.get(
        area_id
    )

    if (
        previous is not None
        and previous == value
    ):
        return

    try:
        from functions.motionAware import (
            setMotionAwareRuntimeMotion,
        )

        setMotionAwareRuntimeMotion(
            area_id,
            value,
            source="zigbee-lqi",
        )

        _motionAwareRadioState[
            area_id
        ] = value

    except Exception:
        logging.exception(
            "MotionAware radio state "
            "failed area=%s value=%s",
            area_id,
            value,
        )


def _motionaware_radio_confirm_active_topics(
    area_id,
    active_topics,
    observed_at,
):
    """Confirm a coherent RF change across adjacent poll rounds.

    A single LQI response can change due to polling, routing or ordinary
    device telemetry.  Motion therefore requires a stable pair of selected
    participant links over several timely rounds.  This is deliberately
    independent of device names, rooms and MQTT topic naming.
    """
    active_topics = set(active_topics)

    with _motionAwareRadioLock:
        if len(active_topics) < 2:
            _motionAwareRadioConfirmation.pop(area_id, None)
            return False

        previous = _motionAwareRadioConfirmation.get(area_id)
        if (
            not isinstance(previous, dict)
            or observed_at - previous.get(
                "observed_at",
                float("-inf"),
            ) > _MOTIONAWARE_CONFIRMATION_MAX_GAP
        ):
            _motionAwareRadioConfirmation[area_id] = {
                "topics": active_topics,
                "rounds": 1,
                "observed_at": observed_at,
            }
            return False

        shared_topics = set(
            previous.get("topics", ())
        ) & active_topics
        if len(shared_topics) < 2:
            _motionAwareRadioConfirmation[area_id] = {
                "topics": active_topics,
                "rounds": 1,
                "observed_at": observed_at,
            }
            return False

        rounds = previous.get("rounds", 0) + 1
        _motionAwareRadioConfirmation[area_id] = {
            "topics": shared_topics,
            "rounds": rounds,
            "observed_at": observed_at,
        }
        # Edge-trigger confirmation.  A sustained route change may produce
        # one motion report but must not refresh the hold timer forever.
        # A quiet/non-coherent round clears this state and rearms detection.
        return rounds == _MOTIONAWARE_CONFIRMATION_ROUNDS


def _motionaware_radio_apply_hold_state(
    area_id,
    observed_at,
):
    """Expire a prior TRUE state even when this MQTT round is incomplete."""
    with _motionAwareRadioLock:
        previous = _motionAwareRadioState.get(area_id)
        last_motion = _motionAwareRadioLastMotion.get(
            area_id,
            float("-inf"),
        )

    motion = (
        observed_at - last_motion
        <= _MOTIONAWARE_HOLD_SECONDS
    )

    # Do not create a synthetic initial FALSE event.  A quiet resource is
    # already rendered by motionAware.py until a real RF transition occurs.
    if previous is not None or motion:
        _motionaware_radio_set_state(area_id, motion)

    return motion


def _motionaware_radio_evaluate(
    spec,
    round_started
):
    area_id = spec["area_id"]

    fresh = {}

    with _motionAwareRadioLock:
        for topic in spec["topics"]:
            sample = (
                _motionAwareRadioSamples
                .get(topic)
            )

            if sample is None:
                continue

            timestamp, value = sample

            # Critical: reject samples from an earlier poll cycle.
            if timestamp >= round_started:
                fresh[topic] = value

    now = time.monotonic()

    # An incomplete selected graph cannot establish a new RF event, but it
    # still must be allowed to expire a previous TRUE hold.
    if len(fresh) < len(spec["topics"]):
        _motionaware_radio_confirm_active_topics(
            area_id,
            set(),
            now,
        )
        _motionaware_radio_apply_hold_state(area_id, now)
        return

    absolute_threshold, sigma_multiplier = (
        _motionaware_radio_thresholds(
            spec["sensitivity"]
        )
    )

    scored = {}

    for topic, value in fresh.items():
        key = (
            area_id,
            topic
        )

        history = (
            _motionAwareRadioHistory
            .setdefault(
                key,
                deque(
                    maxlen=(
                        _MOTIONAWARE_BASELINE_SAMPLES
                    )
                )
            )
        )

        if (
            len(history)
            < _MOTIONAWARE_BASELINE_SAMPLES
        ):
            # The production worker originally trained these samples in the
            # quiet-period block below.  The full-participant guard now
            # returns early during warmup, so retain that one-sample-per-round
            # calibration behaviour explicitly here.
            history.append(value)
            continue

        baseline = statistics.median(
            history
        )

        spread = (
            _motionaware_robust_spread(
                history
            )
        )

        delta = abs(
            value - baseline
        )

        threshold = max(
            absolute_threshold,
            sigma_multiplier * spread,
        )

        scored[topic] = {
            "value": value,
            "delta": delta,
            "threshold": threshold,
            "active": delta >= threshold,
        }

    active_topics = {
        topic
        for topic, item in scored.items()
        if item["active"]
    }
    active_count = len(active_topics)

    # A configured MotionAware area is a calibrated 3--4 participant graph.
    # Never derive motion from a partial baseline merely because two devices
    # replied first: route changes and device reachability otherwise create
    # false positives.
    if len(scored) < len(spec["topics"]):
        _motionaware_radio_confirm_active_topics(
            area_id,
            set(),
            now,
        )
        _motionaware_radio_apply_hold_state(area_id, now)
        return

    detected = _motionaware_radio_confirm_active_topics(
        area_id,
        active_topics,
        now,
    )

    if detected:
        _motionAwareRadioLastMotion[
            area_id
        ] = now

    motion = _motionaware_radio_apply_hold_state(
        area_id,
        now,
    )

    previous = (
        _motionAwareRadioState
        .get(
            area_id,
            False
        )
    )

    if motion != previous:
        details = ", ".join(
            "{}={:.0f}/Δ{:.1f}/T{:.1f}{}".format(
                topic.rsplit("/", 1)[-1],
                item["value"],
                item["delta"],
                item["threshold"],
                "*" if item["active"] else "",
            )
            for topic, item
            in sorted(scored.items())
        )

        logging.info(
            "MotionAware radio area=%s "
            "name=%s motion=%s "
            "sensitivity=%s active=%s [%s]",
            area_id,
            spec["name"],
            motion,
            spec["sensitivity"],
            active_count,
            details,
        )

        _motionaware_radio_set_state(
            area_id,
            motion
        )

    # Quiet periods train/adapt the baseline.
    if not motion:
        for topic, value in fresh.items():
            # Do not teach active anomalies into the quiet baseline.  That
            # keeps ordinary LQI jitter and rejected motion out of calibration.
            if topic in active_topics:
                continue
            key = (
                area_id,
                topic
            )

            history = (
                _motionAwareRadioHistory
                .setdefault(
                    key,
                    deque(
                        maxlen=(
                            _MOTIONAWARE_BASELINE_SAMPLES
                        )
                    )
                )
            )

            history.append(value)


def _motionaware_radio_worker(
    mqtt_client
):
    global _motionAwareRadioWorkerRunning

    logging.info(
        "MotionAware radio worker started"
    )

    try:
        while True:
            specs = (
                _motionaware_radio_area_specs()
            )

            if not specs:
                sleep(
                    _MOTIONAWARE_POLL_INTERVAL
                )
                continue

            round_started = (
                time.monotonic()
            )

            # Ask each configured participant for live Zigbee state.
            for spec in specs:
                for topic in spec["topics"]:
                    result = mqtt_client.publish(
                        topic + "/get",
                        json.dumps({
                            "state": ""
                        }),
                    )

                    if (
                        result.rc
                        != mqtt.MQTT_ERR_SUCCESS
                    ):
                        logging.debug(
                            "MotionAware poll failed "
                            "topic=%s rc=%s",
                            topic,
                            result.rc,
                        )

            _motionaware_radio_wait_for_samples(
                specs,
                round_started,
            )

            for spec in specs:
                try:
                    _motionaware_radio_evaluate(
                        spec,
                        round_started,
                    )
                except Exception:
                    logging.exception(
                        "MotionAware radio "
                        "evaluation failed area=%s",
                        spec["area_id"],
                    )

            elapsed = (
                time.monotonic()
                - round_started
            )

            sleep(
                max(
                    0.05,
                    _MOTIONAWARE_POLL_INTERVAL
                    - elapsed
                )
            )

    except Exception:
        logging.exception(
            "MotionAware radio worker crashed"
        )

    finally:
        _motionAwareRadioWorkerRunning = False


def _start_motionaware_radio_worker(
    mqtt_client
):
    global _motionAwareRadioWorkerRunning

    if not _is_motionaware_radio_enabled():
        return False

    if _motionAwareRadioWorkerRunning:
        return False

    _motionAwareRadioWorkerRunning = True

    Thread(
        target=_motionaware_radio_worker,
        args=(mqtt_client,),
        name="diyHue-MotionAware-radio",
        daemon=True,
    ).start()

    return True


motionSensors = ["TRADFRI motion sensor", "lumi.sensor_motion.aq2", "lumi.sensor_motion", "lumi.motion.ac02", "SML001"]
standardSensors = {
    "TRADFRI remote control": {
        "dataConversion": {
            "rootKey": "action",
            "toggle": {"buttonevent": 1002},
            "arrow_right_click": {"buttonevent": 5002},
            "arrow_right_hold": {"buttonevent": 5001},
            "arrow_left_click": {"buttonevent": 4002},
            "arrow_left_hold": {"buttonevent": 4001},
            "brightness_up_click": {"buttonevent": 2002},
            "brightness_up_hold": {"buttonevent": 2001},
            "brightness_down_click": {"buttonevent": 3002},
            "brightness_down_hold": {"buttonevent": 3001},
            "brightness_up_release": {"buttonevent": 2003},
            "brightness_down_release": {"buttonevent": 3003},
            "arrow_left_release": {"buttonevent": 4003},
            "arrow_right_release": {"buttonevent": 5003},
        }
    },
    "TRADFRI on/off switch": {
        "dataConversion": {
            "rootKey": "action",
            "on": {"buttonevent": 1002},
            "off": {"buttonevent": 2002},
            "brightness_up": {"buttonevent": 1001},
            "brightness_down": {"buttonevent": 2001},
            "brightness_stop": {"buttonevent": 3001},
        }
    },
    "TRADFRI wireless dimmer": {
        "dataConversion": {
            "rootKey": "action",
            "rotate_right_quick": {"buttonevent": 1002},
            "rotate_right": {"buttonevent": 2002},
            "rotate_left": {"buttonevent": 3002},
            "rotate_left_quick": {"buttonevent": 4002},
            "rotate_stop": {},
            "": {},
        }
    },
    "RWL021": {
        "dataConversion": {
            "rootKey": "action",
            "on_press": {"buttonevent": 1000},
            "on-press": {"buttonevent": 1000},
            "on_hold": {"buttonevent": 1001},
            "on-hold": {"buttonevent": 1001},
            "on_press_release": {"buttonevent": 1002},
            "on-press-release": {"buttonevent": 1002},
            "on_hold_release": {"buttonevent": 1003},
            "on-hold-release": {"buttonevent": 1003},
            "up_press": {"buttonevent": 2000},
            "up-press": {"buttonevent": 2000},
            "up_hold": {"buttonevent": 2001},
            "up-hold": {"buttonevent": 2001},
            "up_press_release": {"buttonevent": 2002},
            "up-press-release": {"buttonevent": 2002},
            "up_hold_release": {"buttonevent": 2003},
            "up-hold-release": {"buttonevent": 2003},
            "down_press": {"buttonevent": 3000},
            "down-press": {"buttonevent": 3000},
            "down_hold": {"buttonevent": 3001},
            "down-hold": {"buttonevent": 3001},
            "down_press_release": {"buttonevent": 3002},
            "down-press-release": {"buttonevent": 3002},
            "down_hold_release": {"buttonevent": 3003},
            "down-hold-release": {"buttonevent": 3003},
            "off_press": {"buttonevent": 4000},
            "off-press": {"buttonevent": 4000},
            "off_hold": {"buttonevent": 4001},
            "off-hold": {"buttonevent": 4001},
            "off_press_release": {"buttonevent": 4002},
            "off-press-release": {"buttonevent": 4002},
            "off_hold_release": {"buttonevent": 4003},
            "off-hold-release": {"buttonevent": 4003},
        }
    },
    "WXKG01LM": {
        "dataConversion": {
            "rootKey": "action",
            "single": {"buttonevent": 1001},
            "double": {"buttonevent": 1002},
            "triple": {"buttonevent": 1003},
            "quadruple": {"buttonevent": 1004},
            "hold": {"buttonevent": 2001},
            "release": {"buttonevent": 2002},
            "release": {"many": 2003},
        }
    },
    "Remote Control N2": {
        "dataConversion": {
            "rootKey": "action",
            "on": {"buttonevent": 1001},
            "off": {"buttonevent": 2001},
            "brightness_move_up": {"buttonevent": 1002},
            "brightness_stop": {"buttonevent": 1003},
            "brightness_move_down": {"buttonevent": 2002},
            "arrow_left_click": {"buttonevent": 3002},
            "arrow_right_click": {"many": 4002},
        }
    },
    "RDM002": {
        "dataConversion": {
            "rootKey": "action",
            "dirKey": "action_direction",
            "typeKey": "action_type",
            "timeKey": "action_time",
            "button_1_press": {"buttonevent": 1000},
            "button_1_hold": {"buttonevent": 1001},
            "button_1_press_release": {"buttonevent": 1002},
            "button_1_hold_release": {"buttonevent": 1003},
            "button_2_press": {"buttonevent": 2000},
            "button_2_hold": {"buttonevent": 2001},
            "button_2_press_release": {"buttonevent": 2002},
            "button_2_hold_release": {"buttonevent": 2003},
            "button_3_press": {"buttonevent": 3000},
            "button_3_hold": {"buttonevent": 3001},
            "button_3_press_release": {"buttonevent": 3002},
            "button_3_hold_release": {"buttonevent": 3003},
            "button_4_press": {"buttonevent": 4000},
            "button_4_hold": {"buttonevent": 4001},
            "button_4_press_release": {"buttonevent": 4002},
            "button_4_hold_release": {"buttonevent": 4003},
            "dial_rotate_left_step": {"rotaryevent": 1},
            "dial_rotate_left_slow": {"rotaryevent": 2},
            "dial_rotate_left_fast": {"rotaryevent": 2},
            "dial_rotate_right_step": {"rotaryevent": 1},
            "dial_rotate_right_slow": {"rotaryevent": 2},
            "dial_rotate_right_fast": {"rotaryevent": 2},
            "brightness_step_up": {"rotaryevent": 1},
            "brightness_step_down": {"rotaryevent": 1},
            "rotaryevent": {},
            "expectedrotation":90,
            "expectedeventduration":400
        }
    },
    "PTM 215Z": {
        "dataConversion": {
            "rootKey": "action",
            "press_1": {"buttonevent": 1000},
            "release_1": {"buttonevent": 1002},
            "press_2": {"buttonevent": 2000},
            "release_2": {"buttonevent": 2002},
            "press_3": {"buttonevent": 3000},
            "release_3": {"buttonevent": 3002},
            "press_4": {"buttonevent": 4000},
            "release_4": {"buttonevent": 4002},
            "press_1_and_3": {"buttonevent": 1010},
            "release_1_and_3": {"buttonevent": 1003},
            "press_2_and_4": {"buttonevent": 2010},
            "release_2_and_4": {"buttonevent": 2003},
            "press_energy_bar": {"buttonevent": 5000},
        }
    },
}



# WXKG01LM MiJia wireless switch https://www.zigbee2mqtt.io/devices/WXKG01LM.html

# Register the same model aliases used in sensorTypes so that MQTT auto-discovery
# can look up MQTT action mappings by the same alternative model IDs.
for _alias, _canonical in MODEL_ALIASES.items():
    if _canonical in standardSensors:
        standardSensors[_alias] = standardSensors[_canonical]


def getClient():
    return client

def longPressButton(sensor, buttonevent):
    print("running.....")
    logging.info("long press detected")
    sleep(1)
    while sensor.state["buttonevent"] == buttonevent:
        logging.info("still pressed")
        current_time =  datetime.now()
        sensor.dxState["lastupdated"] = current_time
        rulesProcessor(sensor, current_time)
        checkBehaviorInstances(sensor)
        sleep(0.5)
    return

def streamGroupEvent(device, state):
    for id, group in bridgeConfig["groups"].items():
        if id != "0":
            for light in group.lights:
                if light().id_v1 == device.id_v1:
                    group.genStreamEvent(state)


def getObject(friendly_name):
    if friendly_name in devices_ids:
        logging.debug("Cache Hit for " + friendly_name)
        return devices_ids[friendly_name]()
    else:
        for resource in ["sensors", "lights"]:
            for key, device in bridgeConfig[resource].items():
                if device.protocol == "mqtt":
                    if "friendly_name" in device.protocol_cfg and device.protocol_cfg["friendly_name"] == friendly_name:
                        if device.modelid == "SML001" and device.type != "ZLLPresence":
                            continue
                        devices_ids[friendly_name] = weakref.ref(device)
                        logging.debug("Cache Miss " + friendly_name)
                        return device
                    elif "state_topic" in device.protocol_cfg and device.protocol_cfg["state_topic"] == "zigbee2mqtt/" + friendly_name:
                        devices_ids[friendly_name] = weakref.ref(device)
                        logging.debug("Cache Miss " + friendly_name)
                        return device
        logging.debug("Device not found for " + friendly_name)
        return False


def getObjectByStateTopic(topic):
    """
    Resolve an MQTT light/sensor using its exact configured state_topic.
    """
    for resource in ["lights", "sensors"]:
        for _key, device in bridgeConfig[resource].items():
            if device.protocol != "mqtt":
                continue

            if device.protocol_cfg.get("state_topic") == topic:
                return device

    return False


def getObjectsByAvailabilityTopic(topic):
    """
    Find MQTT lights represented by an availability topic.
    """
    result = []

    for _key, device in bridgeConfig["lights"].items():
        if device.protocol != "mqtt":
            continue

        cfg = device.protocol_cfg

        topics = cfg.get(
            "z2m_availability_topics",
            []
        )

        if not isinstance(topics, list):
            topics = []

        if topic in topics:
            result.append(device)
            continue

        state_topic = cfg.get("state_topic")

        if (
            isinstance(state_topic, str)
            and topic == state_topic + "/availability"
        ):
            result.append(device)

    return result


def _availability_state(data):
    """
    Normalize JSON/string availability payloads to True/False/None.
    """
    if isinstance(data, dict):
        value = data.get("state")
    else:
        value = data

    if value is None:
        return None

    value = str(value).strip().lower()

    if value in (
        "online",
        "available",
        "on",
        "true",
        "1",
    ):
        return True

    if value in (
        "offline",
        "unavailable",
        "off",
        "false",
        "0",
    ):
        return False

    return None


def _set_mqtt_reachable(device, reachable):
    """
    Change Hue reachability only when physical connectivity changes.
    """
    reachable = bool(reachable)

    previous = bool(
        device.state.get("reachable")
    )

    if previous == reachable:
        return False

    device.state["reachable"] = reachable

    logging.info(
        "MQTT reachability %s -> %s",
        device.name,
        (
            "connected"
            if reachable
            else "connectivity_issue"
        ),
    )

    try:
        device.genZigBeeStreamEvent()

    except AttributeError:
        pass

    except Exception as e:
        logging.warning(
            "MQTT connectivity SSE failed for %s: %s",
            device.name,
            e,
        )

    return True



def _mqtt_state_get_payload(device):
    """
    Build a Zigbee2MQTT /get request from physical capabilities.
    """
    cfg = device.protocol_cfg

    caps = cfg.get(
        "z2m_capabilities",
        {}
    )

    if not isinstance(caps, dict):
        caps = {}

    payload = {
        "state": ""
    }

    if caps.get("brightness") is True:
        payload["brightness"] = ""

    modes = caps.get(
        "supported_color_modes",
        []
    )

    if not isinstance(modes, list):
        modes = []

    if "color_temp" in modes:
        payload["color_temp"] = ""

    if "xy" in modes:
        payload["color"] = {
            "x": "",
            "y": ""
        }

    return payload


def _mqtt_state_get_topic(device):
    """
    /get is a Zigbee2MQTT contract, so only use it for confirmed Z2M
    lights. Generic MQTT lights must never receive an invented /get.
    """
    cfg = device.protocol_cfg

    state_topic = cfg.get(
        "state_topic"
    )

    if not isinstance(state_topic, str):
        return None

    if not state_topic.startswith(
        "zigbee2mqtt/"
    ):
        return None

    if not cfg.get("z2m_device"):
        return None

    return state_topic + "/get"


def _refresh_mqtt_light_states(
    mqtt_client,
    reason="manual"
):
    """
    Query physical state from every Zigbee2MQTT light.

    Requests are deliberately paced so startup does not create a large
    simultaneous Zigbee read burst.
    """
    global mqttStateSyncRunning

    requested = 0
    skipped = 0
    failed = 0

    try:
        # Allow retained discovery / availability / bridge-state
        # messages to settle first.
        time.sleep(1.5)

        lights = list(
            bridgeConfig["lights"].items()
        )

        lights.sort(
            key=lambda item: (
                int(item[0])
                if str(item[0]).isdigit()
                else 999999
            )
        )

        for _light_id, device in lights:

            if device.protocol != "mqtt":
                continue

            # Abort promptly if Z2M goes offline during the refresh.
            if mqttBridgeOnline is False:
                logging.info(
                    "MQTT state recovery aborted: "
                    "Zigbee2MQTT bridge offline"
                )
                break

            topic = _mqtt_state_get_topic(
                device
            )

            if topic is None:
                skipped += 1
                continue

            payload = _mqtt_state_get_payload(
                device
            )

            try:
                result = mqtt_client.publish(
                    topic,
                    json.dumps(payload),
                )

                rc = getattr(
                    result,
                    "rc",
                    0,
                )

                if rc != 0:
                    failed += 1

                    logging.warning(
                        "MQTT state recovery publish "
                        "failed for %s rc=%s",
                        device.name,
                        rc,
                    )

                else:
                    requested += 1

            except Exception as e:
                failed += 1

                logging.warning(
                    "MQTT state recovery failed "
                    "for %s: %s",
                    device.name,
                    e,
                )

            # 32 lamps => ~8 seconds total request pacing.
            time.sleep(0.25)

        logging.info(
            "MQTT state recovery complete "
            "(reason=%s requested=%s skipped=%s failed=%s)",
            reason,
            requested,
            skipped,
            failed,
        )

    finally:
        mqttStateSyncRunning = False


def _schedule_mqtt_state_sync(
    mqtt_client,
    reason
):
    """
    Schedule one physical state refresh. Reconnect storms must not
    produce overlapping full-network reads.
    """
    global mqttStateSyncRunning

    if mqttStateSyncRunning:
        logging.debug(
            "MQTT state recovery already running; "
            "ignoring reason=%s",
            reason,
        )
        return False

    mqttStateSyncRunning = True

    Thread(
        target=_refresh_mqtt_light_states,
        args=(
            mqtt_client,
            reason,
        ),
        name="diyHue-MQTT-state-recovery",
        daemon=True,
    ).start()

    logging.info(
        "MQTT state recovery scheduled "
        "(reason=%s)",
        reason,
    )

    return True


def _handle_mqtt_bridge_state(
    data,
    mqtt_client=None
):
    global mqttBridgeOnline

    online = _availability_state(data)

    if online is None:
        return

    previous = mqttBridgeOnline
    changed = previous != online

    mqttBridgeOnline = online

    if changed:
        logging.info(
            "Zigbee2MQTT bridge is %s",
            "online" if online else "offline",
        )

    # If Z2M itself is offline, none of its devices can truthfully
    # be reported as reachable.
    if not online:
        for _key, device in bridgeConfig["lights"].items():
            if device.protocol == "mqtt":
                _set_mqtt_reachable(
                    device,
                    False,
                )

        return

    # Z2M can restart while diyHue stays connected to Mosquitto.
    # on_connect() is then never invoked, therefore a transition back
    # to online must independently trigger physical state recovery.
    if (
        mqtt_client is not None
        and changed
    ):
        _schedule_mqtt_state_sync(
            mqtt_client,
            "zigbee2mqtt-online",
        )


def _handle_mqtt_device_availability(topic, data):
    online = _availability_state(data)

    if online is None:
        return False

    devices = getObjectsByAvailabilityTopic(
        topic
    )

    if not devices:
        return False

    # An explicit ONLINE from a device is considered fresh physical
    # evidence. A later bridge/offline event will override it if needed.
    for device in devices:
        _set_mqtt_reachable(
            device,
            online,
        )

    return True


def getSensorsByIeeeAddr(ieee_address):
    """Return all sensor objects whose protocol_cfg.ieeeAddr matches."""
    result = []
    for key, device in bridgeConfig["sensors"].items():
        if device.protocol == "mqtt" and device.protocol_cfg.get("ieeeAddr") == ieee_address:
            result.append(device)
    return result

# ---------------------------------------------------------------------------
# MQTT / Zigbee2MQTT light capability normalization
# ---------------------------------------------------------------------------

def _extract_mqtt_light_capabilities(data):
    """
    Normalize both legacy and current Home Assistant MQTT light discovery.

    Older Zigbee2MQTT discovery exposed:
        xy: true
        color_temp: true

    Current Zigbee2MQTT exposes:
        supported_color_modes: ["xy", "color_temp"]

    Keep the normalized physical capabilities in protocol_cfg so the Hue
    API layer can describe the real lamp rather than blindly assuming the
    generic Hue model template is physically accurate.
    """

    modes = data.get("supported_color_modes")

    if not isinstance(modes, list):
        modes = []

    modes = [
        str(mode)
        for mode in modes
        if mode is not None
    ]

    # Backwards compatibility with old Z2M / HA discovery.
    if data.get("xy") is True and "xy" not in modes:
        modes.append("xy")

    if (
        data.get("color_temp") is True
        and "color_temp" not in modes
    ):
        modes.append("color_temp")

    if (
        data.get("brightness") is True
        and not modes
    ):
        # Brightness-only lamps normally appear as brightness.
        modes.append("brightness")

    def as_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    effect_list = data.get("effect_list")

    if not isinstance(effect_list, list):
        effect_list = []

    return {
        "brightness": data.get("brightness") is True,
        "brightness_scale": (
            as_int(data.get("brightness_scale"))
            or 254
        ),
        "supported_color_modes": modes,
        "min_mireds": as_int(data.get("min_mireds")),
        "max_mireds": as_int(data.get("max_mireds")),
        "effect": data.get("effect") is True,
        "effect_list": [
            str(effect)
            for effect in effect_list
        ],
    }



def _extract_mqtt_light_availability_topics(data):
    """
    Preserve all MQTT availability topics announced by discovery.
    """
    result = []

    availability = data.get("availability")

    if isinstance(availability, list):
        for entry in availability:

            if isinstance(entry, dict):
                topic = entry.get("topic")

            elif isinstance(entry, str):
                topic = entry

            else:
                topic = None

            if (
                isinstance(topic, str)
                and topic
                and topic not in result
            ):
                result.append(topic)

    elif isinstance(availability, dict):
        topic = availability.get("topic")

        if (
            isinstance(topic, str)
            and topic
        ):
            result.append(topic)

    legacy = data.get(
        "availability_topic"
    )

    if (
        isinstance(legacy, str)
        and legacy
        and legacy not in result
    ):
        result.append(legacy)

    return result


def _extract_mqtt_light_device(data):
    """
    Preserve physical Zigbee2MQTT device identity separately from the
    Hue compatibility model used by diyHue.
    """

    device = data.get("device")

    if not isinstance(device, dict):
        device = {}

    return {
        "manufacturer": device.get("manufacturer"),
        "model": device.get("model"),
        "model_id": device.get("model_id"),
        "sw_version": device.get("sw_version"),
        "hw_version": device.get("hw_version"),
    }


def _mqtt_light_model_from_capabilities(capabilities):
    """
    Select diyHue's compatibility template from normalized capabilities.

    The physical identity is stored separately in protocol_cfg.
    """

    modes = capabilities.get(
        "supported_color_modes",
        []
    )

    has_xy = "xy" in modes
    has_ct = "color_temp" in modes
    has_bri = capabilities.get("brightness") is True

    if has_xy and has_ct:
        return "LCT015"

    if has_xy:
        return "LLC010"

    if has_ct:
        return "LTW001"

    if has_bri:
        return "LWB010"

    return "LOM001"


def _mqtt_light_friendly_name(data):
    object_id = data.get("object_id")

    if object_id:
        return str(object_id)

    state_topic = data.get("state_topic")

    if (
        isinstance(state_topic, str)
        and state_topic.startswith("zigbee2mqtt/")
    ):
        return state_topic[len("zigbee2mqtt/"):]

    return None


def _update_existing_mqtt_light_from_discovery(obj, data):
    """
    Refresh discovery metadata for an already-known light.

    Upstream historically refreshed only state_topic/command_topic,
    which meant capability changes were discarded forever.
    """

    capabilities = _extract_mqtt_light_capabilities(data)
    device = _extract_mqtt_light_device(data)

    updates = {
        "command_topic": data.get("command_topic"),
        "state_topic": data.get("state_topic"),
        "friendly_name": _mqtt_light_friendly_name(data),
        "z2m_capabilities": capabilities,
        "z2m_device": device,
        "z2m_availability_topics": (
            _extract_mqtt_light_availability_topics(data)
        ),
    }

    changed = False

    for key, value in updates.items():
        if value is None:
            continue

        if obj.protocol_cfg.get(key) != value:
            obj.protocol_cfg[key] = value
            changed = True

    if changed:
        configManager.bridgeConfig.mark_dirty("lights")

    return changed


# Will get called zero or more times depending on how many lights are
# available for autodiscovery.

def _topic_covered_by_core_subscription(topic):
    if not isinstance(topic, str):
        return True

    parts = topic.split("/")

    # zigbee2mqtt/+
    if (
        len(parts) == 2
        and parts[0] == "zigbee2mqtt"
    ):
        return True

    # zigbee2mqtt/+/availability
    if (
        len(parts) == 3
        and parts[0] == "zigbee2mqtt"
        and parts[2] == "availability"
    ):
        return True

    if topic == "zigbee2mqtt/bridge/state":
        return True

    return False


def _subscribe_exact_topic(topic, mqtt_client=None):
    if (
        not isinstance(topic, str)
        or not topic
        or _topic_covered_by_core_subscription(topic)
    ):
        return

    mqtt_client = mqtt_client or client

    mqtt_client.subscribe(topic)

    logging.debug(
        "MQTT exact subscription: %s",
        topic,
    )


def _subscribe_mqtt_object_topics(device, mqtt_client=None):
    if device.protocol != "mqtt":
        return

    cfg = device.protocol_cfg

    _subscribe_exact_topic(
        cfg.get("state_topic"),
        mqtt_client,
    )

    topics = cfg.get(
        "z2m_availability_topics",
        [],
    )

    if isinstance(topics, list):
        for topic in topics:
            _subscribe_exact_topic(
                topic,
                mqtt_client,
            )


def _subscribe_mqtt_discovery_topics(data):
    _subscribe_exact_topic(
        data.get("state_topic")
    )

    for topic in _extract_mqtt_light_availability_topics(data):
        _subscribe_exact_topic(topic)



# ===========================================================================
# Generic Zigbee2MQTT -> Hue API v1 sensor compatibility
# ===========================================================================

GENERIC_Z2M_SENSOR_SPECS = {
    "DS01": [
        {
            "type": "ZHAOpenClose",
            "cluster": "0006",
            "suffix": "",
            "state": {
                "open": False,
                "lastupdated": "none",
            },
        },
    ],

    "lumi.weather": [
        {
            "type": "ZHATemperature",
            "cluster": "0402",
            "suffix": " temperatur",
            "state": {
                "temperature": 0,
                "lastupdated": "none",
            },
        },
        {
            "type": "ZHAHumidity",
            "cluster": "0405",
            "suffix": " luftfuktighet",
            "state": {
                "humidity": 0,
                "lastupdated": "none",
            },
        },
    ],

    "SmokeSensor-EF-3.0": [
        {
            "type": "ZHAFire",
            "cluster": "0500",
            "suffix": "",
            "state": {
                "fire": False,
                "lastupdated": "none",
            },
        },
    ],
}


def _generic_z2m_sensor_specs(device_data):
    if not isinstance(device_data, dict):
        return None

    return GENERIC_Z2M_SENSOR_SPECS.get(
        device_data.get("model_id")
    )


def _generic_z2m_uniqueid(
    ieee_address,
    cluster
):
    return (
        convertHexToMac(ieee_address)
        + "-01-"
        + cluster
    )


def _generic_z2m_sensor_cfg(device_data):
    friendly = device_data.get(
        "friendly_name"
    )

    definition = (
        device_data.get("definition")
        or {}
    )

    state_topic = (
        "zigbee2mqtt/" + friendly
        if isinstance(friendly, str)
        else None
    )

    availability = []

    if state_topic:
        availability.append(
            state_topic + "/availability"
        )

    return {
        "friendly_name": friendly,
        "ieeeAddr": device_data.get(
            "ieee_address"
        ),
        "model": definition.get("model"),
        "vendor": definition.get("vendor"),
        "state_topic": state_topic,
        "z2m_availability_topics": availability,
        "generic_z2m_sensor": True,
    }


def _find_generic_z2m_sensor(
    ieee_address,
    sensor_type
):
    for _sid, sensor in bridgeConfig["sensors"].items():

        if sensor.protocol != "mqtt":
            continue

        if sensor.type != sensor_type:
            continue

        cfg = sensor.protocol_cfg

        if not cfg.get(
            "generic_z2m_sensor"
        ):
            continue

        if cfg.get(
            "ieeeAddr"
        ) == ieee_address:
            return sensor

    return None


def _discover_generic_z2m_sensor(
    device_data
):
    specs = _generic_z2m_sensor_specs(
        device_data
    )

    if not specs:
        return False

    ieee = device_data.get(
        "ieee_address"
    )

    friendly = device_data.get(
        "friendly_name"
    )

    model_id = device_data.get(
        "model_id"
    )

    if not ieee or not friendly:
        logging.warning(
            "MQTT generic sensor skipped: "
            "missing IEEE/friendly_name model=%s",
            model_id,
        )
        return True

    definition = (
        device_data.get("definition")
        or {}
    )

    manufacturer = (
        definition.get("vendor")
        or "Unknown"
    )

    swversion = (
        device_data.get(
            "software_build_id"
        )
        or ""
    )

    cfg = _generic_z2m_sensor_cfg(
        device_data
    )

    changed = False

    for spec in specs:

        sensor_type = spec["type"]

        sensor = _find_generic_z2m_sensor(
            ieee,
            sensor_type,
        )

        desired_name = (
            str(friendly)
            + spec.get("suffix", "")
        )[:40]

        if sensor is not None:

            if sensor.name != desired_name:
                sensor.name = desired_name
                changed = True

            if (
                sensor.manufacturername
                != manufacturer
            ):
                sensor.manufacturername = manufacturer
                changed = True

            for key, value in cfg.items():

                if (
                    sensor.protocol_cfg.get(key)
                    != value
                ):
                    sensor.protocol_cfg[key] = value
                    changed = True

            continue

        sid = nextFreeId(
            bridgeConfig,
            "sensors",
        )

        sensor_data = {
            "name": desired_name,
            "id_v1": sid,
            "protocol": "mqtt",
            "protocol_cfg": dict(cfg),
            "modelid": model_id,
            "type": sensor_type,
            "uniqueid": _generic_z2m_uniqueid(
                ieee,
                spec["cluster"],
            ),
            "manufacturername": manufacturer,
            "swversion": swversion,
            "config": {
                "on": True,
                "battery": 100,
                "reachable": True,
            },
            "state": dict(
                spec["state"]
            ),
        }

        bridgeConfig["sensors"][sid] = (
            Sensor.Sensor(
                sensor_data
            )
        )

        logging.info(
            "MQTT: added generic sensor "
            "%s id=%s model=%s type=%s",
            desired_name,
            sid,
            model_id,
            sensor_type,
        )

        changed = True

    if changed:
        configManager.bridgeConfig.mark_dirty(
            "sensors"
        )

    return True


def _generic_z2m_sensor_payload(
    sensor_type,
    data
):
    result = {}

    if sensor_type == "ZHAOpenClose":

        if "contact" in data:
            # Zigbee2MQTT contact=True means closed.
            result["open"] = not bool(
                data["contact"]
            )

    elif sensor_type == "ZHATemperature":

        value = data.get("temperature")

        if value is not None:
            result["temperature"] = int(
                round(float(value) * 100)
            )

    elif sensor_type == "ZHAHumidity":

        value = data.get("humidity")

        if value is not None:
            result["humidity"] = int(
                round(float(value) * 100)
            )

    elif sensor_type == "ZHAFire":

        if "smoke" in data:
            result["fire"] = bool(
                data["smoke"]
            )

    return result


def _set_generic_sensor_reachable(
    sensor,
    reachable
):
    reachable = bool(reachable)

    previous = bool(
        sensor.config.get(
            "reachable",
            True,
        )
    )

    if previous == reachable:
        return False

    sensor.config[
        "reachable"
    ] = reachable

    configManager.bridgeConfig.mark_dirty(
        "sensors"
    )

    logging.info(
        "MQTT sensor reachability %s -> %s",
        sensor.name,
        (
            "connected"
            if reachable
            else "connectivity_issue"
        ),
    )

    try:
        sensor.genZigBeeStreamEvent()
    except Exception as exc:
        logging.warning(
            "MQTT sensor connectivity SSE failed for %s: %s",
            sensor.name,
            exc,
        )

    return True


def _generic_sensors_for_friendly_name(
    friendly_name
):
    result = []

    for _sid, sensor in bridgeConfig["sensors"].items():

        if sensor.protocol != "mqtt":
            continue

        cfg = sensor.protocol_cfg

        if not cfg.get(
            "generic_z2m_sensor"
        ):
            continue

        if cfg.get(
            "friendly_name"
        ) == friendly_name:
            result.append(sensor)

    return result


def _handle_generic_z2m_sensor_state(
    friendly_name,
    data
):
    sensors = (
        _generic_sensors_for_friendly_name(
            friendly_name
        )
    )

    if not sensors:
        return False

    current_time = datetime.now()

    timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )

    dirty = False

    for sensor in sensors:

        if _set_generic_sensor_reachable(
            sensor,
            True,
        ):
            dirty = True

        battery_changed = False

        battery = data.get(
            "battery"
        )

        if isinstance(
            battery,
            (int, float),
        ):
            battery = max(
                0,
                min(
                    100,
                    int(round(battery)),
                ),
            )

            if (
                sensor.config.get(
                    "battery"
                )
                != battery
            ):
                sensor.config[
                    "battery"
                ] = battery

                battery_changed = True
                dirty = True

        if battery_changed:
            try:
                sensor.genDevicePowerStreamEvent()
            except Exception as exc:
                logging.warning(
                    "MQTT sensor battery SSE failed for %s: %s",
                    sensor.name,
                    exc,
                )

        converted = (
            _generic_z2m_sensor_payload(
                sensor.type,
                data,
            )
        )

        if not converted:
            continue

        changed = any(
            sensor.state.get(key) != value
            for key, value in converted.items()
        )

        if not changed:
            continue

        for key, value in converted.items():
            sensor.dxState[key] = current_time

        converted[
            "lastupdated"
        ] = timestamp

        sensor.dxState[
            "lastupdated"
        ] = current_time

        sensor.state.update(
            converted
        )

        if sensor.type == "ZHAOpenClose":
            try:
                sensor.genContactStreamEvent()
            except Exception as exc:
                logging.warning(
                    "MQTT contact SSE failed for %s: %s",
                    sensor.name,
                    exc,
                )

        logging.info(
            "MQTT: generic sensor state %s %s",
            sensor.name,
            converted,
        )

        rulesProcessor(
            sensor,
            current_time,
        )
        checkBehaviorInstances(sensor)

        dirty = True

    if dirty:
        configManager.bridgeConfig.mark_dirty(
            "sensors"
        )

    return True


def _handle_generic_z2m_availability(
    topic,
    data
):
    reachable = _availability_state(
        data
    )

    if reachable is None:
        return False

    matched = False

    for _sid, sensor in bridgeConfig["sensors"].items():

        if sensor.protocol != "mqtt":
            continue

        cfg = sensor.protocol_cfg

        if not cfg.get(
            "generic_z2m_sensor"
        ):
            continue

        topics = cfg.get(
            "z2m_availability_topics",
            [],
        )

        if topic not in topics:
            continue

        matched = True

        _set_generic_sensor_reachable(
            sensor,
            reachable,
        )

    return matched


def on_autodiscovery_light(msg):
    data = json.loads(msg.payload)

    logging.info(
        "Auto discovery message on: " + msg.topic
    )

    unique_id = data.get("unique_id")

    if not unique_id:
        logging.warning(
            "MQTT light discovery ignored: unique_id missing on %s",
            msg.topic,
        )
        return

    discoveredDevices[unique_id] = data

    # Subscribe immediately if discovery announces custom topics.
    _subscribe_mqtt_discovery_topics(data)

    # First update an existing light. Do not recreate it and do not
    # change its Hue-facing ID/model merely because Z2M discovery
    # formatting changed.
    for light_id, obj in bridgeConfig["lights"].items():
        if obj.protocol != "mqtt":
            continue

        if obj.protocol_cfg.get("uid") != unique_id:
            continue

        changed = _update_existing_mqtt_light_from_discovery(
            obj,
            data,
        )

        if changed:
            caps = obj.protocol_cfg.get(
                "z2m_capabilities",
                {}
            )

            logging.info(
                "MQTT refreshed capabilities for light %s "
                "(id=%s, modes=%s, ct=%s..%s)",
                obj.name,
                light_id,
                caps.get("supported_color_modes"),
                caps.get("min_mireds"),
                caps.get("max_mireds"),
            )

        return

    # New light.
    device_data = data.get("device")

    if not isinstance(device_data, dict):
        device_data = {}

    lightName = (
        device_data.get("name")
        or data.get("name")
        or data.get("object_id")
        or unique_id
    )

    capabilities = _extract_mqtt_light_capabilities(data)
    modelid = _mqtt_light_model_from_capabilities(capabilities)

    logging.info(
        "MQTT: Adding light %s as %s from modes=%s",
        lightName,
        modelid,
        capabilities.get("supported_color_modes"),
    )

    protocol_cfg = {
        "uid": unique_id,
        "ip": "mqtt",
        "state_topic": data.get("state_topic"),
        "command_topic": data.get("command_topic"),
        "friendly_name": _mqtt_light_friendly_name(data),
        "mqtt_server": bridgeConfig["config"]["mqtt"],
        "z2m_capabilities": capabilities,
        "z2m_device": _extract_mqtt_light_device(data),
        "z2m_availability_topics": (
            _extract_mqtt_light_availability_topics(data)
        ),
    }

    addNewLight(
        modelid,
        lightName,
        "mqtt",
        protocol_cfg,
    )


def on_state_update(msg):
    logging.debug("MQTT: got state message on " + msg.topic)
    data = json.loads(msg.payload)
    latestStates[msg.topic] = data
    logging.debug(json.dumps(data, indent=4))

# on_message handler (linked to client below)
def on_message(client, userdata, msg):
    if bridgeConfig["config"]["mqtt"]["enabled"]:
        try:
            current_time =  datetime.now()
            logging.debug("MQTT: got state message on " + msg.topic)
            data = json.loads(msg.payload)

            _motionaware_radio_observe(
                msg.topic,
                data
            )

            logging.debug(msg.payload)
            if msg.topic.startswith(discoveryPrefix + "/light/"):
                on_autodiscovery_light(msg)

            elif msg.topic == "zigbee2mqtt/bridge/state":
                _handle_mqtt_bridge_state(
                    data,
                    client,
                )

            elif msg.topic.endswith("/availability"):
                _handle_generic_z2m_availability(
                    msg.topic,
                    data,
                )

                _handle_mqtt_device_availability(
                    msg.topic,
                    data,
                )

            elif msg.topic == "zigbee2mqtt/bridge/devices":
                for key in data:
                    try:
                        # New generic models are checked BEFORE the old
                        # standardSensors/motionSensors support gate.
                        if _discover_generic_z2m_sensor(key):
                            continue

                        if "model_id" in key and (key["model_id"] in standardSensors or key["model_id"] in motionSensors): # Sensor is supported
                            # Check by IEEE address first to catch sensors registered under a stale friendly name
                            existing = getSensorsByIeeeAddr(key["ieee_address"])
                            if existing:
                                new_name = key["friendly_name"]
                                # Deduplicate: keep one sensor per type, prefer correctly-named entry
                                by_type = {}
                                for s in existing:
                                    by_type.setdefault(s.type, []).append(s)
                                for sensor_type, sensors_of_type in by_type.items():
                                    correct = [s for s in sensors_of_type if s.protocol_cfg.get("friendly_name") == new_name]
                                    stale = [s for s in sensors_of_type if s.protocol_cfg.get("friendly_name") != new_name]
                                    keeper = correct[0] if correct else stale[0]
                                    for s in sensors_of_type:
                                        if s is not keeper:
                                            logging.info("MQTT: removing duplicate sensor %s (id_v1=%s)", s.name, s.id_v1)
                                            devices_ids.pop(s.name, None)
                                            devices_ids.pop(s.protocol_cfg.get("friendly_name"), None)
                                            del bridgeConfig["sensors"][s.id_v1]
                                    if keeper.name != new_name or keeper.protocol_cfg.get("friendly_name") != new_name:
                                        logging.info("MQTT: renaming sensor %s -> %s", keeper.name, new_name)
                                        devices_ids.pop(keeper.name, None)
                                        keeper.name = new_name
                                        keeper.protocol_cfg["friendly_name"] = new_name
                                        devices_ids[new_name] = keeper
                            elif getObject(key["friendly_name"]) == False: ## Add the new sensor
                                logging.info("MQTT: Add new mqtt sensor " + key["friendly_name"])
                                if key["model_id"] in standardSensors:
                                    model_id = key["model_id"]
                                    # MODEL_ALIASES in sensor_types.py already adds every
                                    # variant ID to sensorTypes, so a plain lookup works.
                                    sensor_model_key = model_id

                                    sensor_definitions = sensorTypes.get(
                                        sensor_model_key
                                    )

                                    if not sensor_definitions:
                                        logging.warning(
                                            "MQTT: model %s is in standardSensors "
                                            "but has no sensorTypes definition; "
                                            "device skipped safely",
                                            sensor_model_key,
                                        )
                                        continue

                                    switch_id_v2 = None

                                    for sensor_type in sensor_definitions.keys():
                                        new_sensor_id = nextFreeId(bridgeConfig, "sensors")
                                        uniqueid = convertHexToMac(key["ieee_address"]) + "-01-1000"
                                        sensorData = {"name": key["friendly_name"], "protocol": "mqtt", "modelid": model_id, "type": sensor_type, "uniqueid": uniqueid,"protocol_cfg": {"friendly_name": key["friendly_name"], "ieeeAddr": key["ieee_address"], "model": key["definition"]["model"]}, "id_v1": new_sensor_id}
                                        if sensor_type == "ZLLRelativeRotary" and switch_id_v2:
                                            sensorData["parent_id_v2"] = switch_id_v2
                                        bridgeConfig["sensors"][new_sensor_id] = Sensor.Sensor(sensorData)
                                        if sensor_type != "ZLLRelativeRotary":
                                            switch_id_v2 = bridgeConfig["sensors"][new_sensor_id].id_v2
                                ### TRADFRI Motion Sensor, Xiaomi motion sensor, etc
                                elif key["model_id"] in motionSensors:
                                        logging.info("MQTT: add new motion sensor " + key["model_id"])
                                        addHueMotionSensor(key["friendly_name"], "mqtt", {"modelid": key["model_id"], "lightSensor": "on", "friendly_name": key["friendly_name"]})
                                else:
                                    logging.info("MQTT: unsupported sensor " + key["model_id"])
                    except Exception:
                        # A single malformed device must never abort the
                        # complete retained bridge/devices discovery batch.
                        logging.exception(
                            "MQTT: device discovery failed for "
                            "friendly_name=%s model_id=%s",
                            key.get("friendly_name")
                            if isinstance(key, dict)
                            else None,
                            key.get("model_id")
                            if isinstance(key, dict)
                            else None,
                        )
                        continue
            elif msg.topic == "zigbee2mqtt/bridge/log":
                light = getObject(data["meta"]["friendly_name"])
                if data["type"] == "device_announced":
                    if light.config["startup"]["mode"] == "powerfail":
                        logging.info("set last state for " + light.name)
                        payload = {}
                        payload["state"] = "ON" if light.state["on"] else "OFF"
                        client.publish(light.protocol_cfg['command_topic'], json.dumps(payload))
                elif data["type"] == "zigbee_publish_error":
                    logging.info(light.name + " is unreachable")
                    _set_mqtt_reachable(
                        light,
                        False,
                    )
            else:
                # Prefer exact configured state_topic. Friendly-name
                # lookup remains only as backwards compatibility.
                device = getObjectByStateTopic(
                    msg.topic
                )

                if device is False:
                    device_friendlyname = (
                        msg.topic[
                            msg.topic.index("/") + 1:
                        ]
                    )

                    device = getObject(
                        device_friendlyname
                    )

                else:
                    device_friendlyname = (
                        device.protocol_cfg.get(
                            "friendly_name"
                        )
                        or msg.topic[
                            msg.topic.index("/") + 1:
                        ]
                    )

                # Generic Zigbee2MQTT sensors share their physical
                # MQTT state topic across one or more Hue-v1 resources.
                # device_friendlyname is now guaranteed to be resolved
                # by either exact state_topic routing or fallback routing.
                if _handle_generic_z2m_sensor_state(
                    device_friendlyname,
                    data,
                ):
                    on_state_update(msg)
                    return

                if device != False:
                    if device.getObjectPath()["resource"] == "sensors":
                        if "battery" in data and isinstance(data["battery"], int):
                            device.config["battery"] = data["battery"]
                        if device.config["on"] == False:
                            return
                        convertedPayload = {"lastupdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")}
                        if ("action" in data and data["action"] == "") or ("click" in data and data["click"] == ""):
                            return
                        ### If is a motion sensor update the light level and temperature
                        if device.modelid in motionSensors:
                            convertedPayload["presence"] = data["occupancy"]
                            lightPayload = {"lastupdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")}
                            lightSensor = findLightSensor(device)
                            if "temperature" in data:
                                tempSensor = findTempSensor(device)
                                tempSensor.state = {"temperature": int(data["temperature"] * 100), "lastupdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")}
                            if "illuminance_lux" in data:
                                hue_lightlevel = int(10000 * math.log10(data["illuminance_lux"])) if data["illuminance_lux"] != 0 else 0
                                if hue_lightlevel > lightSensor.config["tholddark"]:
                                    lightPayload["dark"] = False
                                else:
                                    lightPayload["dark"] = True
                                lightPayload["lightlevel"] = hue_lightlevel
                            elif lightSensor.protocol_cfg["lightSensor"] == "on":
                                lightPayload["dark"] = not bridgeConfig["sensors"]["1"].state["daylight"]
                                if  lightPayload["dark"]:
                                    lightPayload["lightlevel"] = 6000
                                else:
                                    lightPayload["lightlevel"] = 25000
                            else: # is always dark
                                lightPayload["dark"] = True
                                lightPayload["lightlevel"] = 6000
                            lightPayload["daylight"] = not lightPayload["dark"]
                            if lightPayload["dark"] != lightSensor.state["dark"]:
                                lightSensor.dxState["dark"] = current_time
                            lightSensor.state.update(lightPayload)
                            # send email if alarm is enabled:
                            if data["occupancy"] and bridgeConfig["config"]["alarm"]["enabled"] and bridgeConfig["config"]["alarm"]["lasttriggered"] + 300 < current_time.timestamp():
                                logging.info("Alarm triggered, sending email...")
                                requests.post("https://diyhue.org/cdn/mailNotify.php", json={"to": bridgeConfig["config"]["alarm"]["email"], "sensor": device.name}, timeout=10)
                                bridgeConfig["config"]["alarm"]["lasttriggered"] = int(current_time.timestamp())
                        elif device.modelid in standardSensors:
                            convertedPayload.update(standardSensors[device.modelid]["dataConversion"][data[standardSensors[device.modelid]["dataConversion"]["rootKey"]]])
                        # For RDM002: extract rotation direction and route rotary events to ZLLRelativeRotary
                        if "rotaryevent" in convertedPayload:
                            action = data.get("action", "")
                            if action.startswith("dial_rotate_"):
                                # brightness_step_* is the canonical per-detent event; skip dial_rotate_* to avoid double-firing
                                return
                            convertedPayload["direction"] = "counter_clock_wise" if "down" in action else "clock_wise"
                            convertedPayload["rotary_step_size"] = data.get("action_step_size", 8)
                            if device.type != "ZLLRelativeRotary":
                                for _k, _s in bridgeConfig["sensors"].items():
                                    if _s.protocol == "mqtt" and _s.protocol_cfg.get("friendly_name") == device_friendlyname and _s.type == "ZLLRelativeRotary":
                                        device = _s
                                        break
                        for key in convertedPayload.keys():
                            if key in device.state and device.state[key] != convertedPayload[key]:
                                device.dxState[key] = current_time
                        device.state.update(convertedPayload)
                        logging.debug(convertedPayload)
                        z2m_sends_hold_repeats = device.modelid in standardSensors and any(
                            k.endswith("_hold") for k in standardSensors[device.modelid]["dataConversion"]
                        )
                        if "buttonevent" in convertedPayload and convertedPayload["buttonevent"] in [1001, 2001, 3001, 4001, 5001] and not z2m_sends_hold_repeats:
                            Thread(target=longPressButton, args=[device, convertedPayload["buttonevent"]]).start()
                        rulesProcessor(device, current_time)
                        checkBehaviorInstances(device)
                    elif device.getObjectPath()["resource"] == "lights":

                        # A real state packet is direct evidence that the
                        # physical light is reachable.
                        _set_mqtt_reachable(
                            device,
                            True,
                        )

                        state = {}
                        v2State = {}

                        # Candle/Fire is emulated by repeatedly changing
                        # the physical IKEA brightness. Do not mirror
                        # those internal animation frames into Hue's
                        # eventstream. A real Hue lamp performs that
                        # animation locally as well.
                        try:
                            protocol_mqtt = importlib.import_module(
                                "lights.protocols.mqtt"
                            )

                            effect_feedback = (
                                protocol_mqtt
                                ._is_emulated_effect_feedback(
                                    device.protocol_cfg[
                                        "command_topic"
                                    ],
                                    data
                                )
                            )
                        except Exception:
                            effect_feedback = False

                        if effect_feedback:
                            on_state_update(msg)
                            return

                        if "state" in data:
                            state["on"] = data["state"] == "ON"
                            v2State["on"] = {
                                "on": state["on"]
                            }

                            # An OFF reported by Zigbee2MQTT may come
                            # from Home Assistant or another external
                            # controller and therefore never passes
                            # through Light.setV1State/setV2State.
                            #
                            # Stop every local animation immediately,
                            # otherwise its next frame can wake the
                            # physical lamp again.
                            if state["on"] is False:
                                device.dynamics["status"] = "none"

                                try:
                                    protocol_mqtt = importlib.import_module(
                                        "lights.protocols.mqtt"
                                    )

                                    protocol_mqtt._stop_emulated_effect(
                                        device.protocol_cfg[
                                            "command_topic"
                                        ]
                                    )

                                except Exception as e:
                                    logging.warning(
                                        "MQTT external OFF effect-stop "
                                        "failed for %s: %s",
                                        device.name,
                                        e,
                                    )

                        if "brightness" in data:
                            state["bri"] = data["brightness"]
                            v2State["dimming"] = {
                                "brightness": round(
                                    state["bri"] / 2.54,
                                    2
                                )
                            }

                        # Zigbee2MQTT may publish BOTH a cached XY
                        # colour and a cached color_temp in the same
                        # payload. color_mode tells us which one is
                        # actually active.
                        color_mode = data.get("color_mode")

                        has_xy = (
                            "color" in data
                            and isinstance(data["color"], dict)
                            and "x" in data["color"]
                            and "y" in data["color"]
                        )

                        has_ct = (
                            "color_temp" in data
                            and data["color_temp"] is not None
                        )

                        if color_mode in ("xy", "hs"):
                            if has_xy:
                                x = float(data["color"]["x"])
                                y = float(data["color"]["y"])

                                state["xy"] = [x, y]
                                state["colormode"] = "xy"

                                v2State["color"] = {
                                    "xy": {
                                        "x": x,
                                        "y": y
                                    }
                                }

                        elif color_mode in ("color_temp", "ct"):
                            if has_ct:
                                state["ct"] = int(
                                    data["color_temp"]
                                )
                                state["colormode"] = "ct"

                                v2State[
                                    "color_temperature"
                                ] = {
                                    "mirek": state["ct"]
                                }

                        else:
                            # Fallback for devices/old Z2M payloads
                            # without color_mode.
                            if has_xy and not has_ct:
                                x = float(data["color"]["x"])
                                y = float(data["color"]["y"])

                                state["xy"] = [x, y]
                                state["colormode"] = "xy"

                                v2State["color"] = {
                                    "xy": {
                                        "x": x,
                                        "y": y
                                    }
                                }

                            elif has_ct and not has_xy:
                                state["ct"] = int(
                                    data["color_temp"]
                                )
                                state["colormode"] = "ct"

                                v2State[
                                    "color_temperature"
                                ] = {
                                    "mirek": state["ct"]
                                }

                            elif has_xy and has_ct:
                                # Ambiguous legacy payload: preserve
                                # current mode if known.
                                if (
                                    device.state.get(
                                        "colormode"
                                    ) == "ct"
                                ):
                                    state["ct"] = int(
                                        data["color_temp"]
                                    )

                                    v2State[
                                        "color_temperature"
                                    ] = {
                                        "mirek": state["ct"]
                                    }

                                else:
                                    x = float(
                                        data["color"]["x"]
                                    )
                                    y = float(
                                        data["color"]["y"]
                                    )

                                    state["xy"] = [x, y]
                                    state["colormode"] = "xy"

                                    v2State["color"] = {
                                        "xy": {
                                            "x": x,
                                            "y": y
                                        }
                                    }

                        if "xy" in state or "ct" in state:
                            device.updateLightState(state)

                        device.state.update(state)

                        # One complete V2 event for the incoming Z2M state.
                        if v2State:
                            device.genStreamEvent(v2State)
                            streamGroupEvent(device, v2State)

                on_state_update(msg)
        except Exception as e:
            logging.info("MQTT Exception | " + str(e))

def findLightSensor(sensor):
    lightSensorUID = sensor.uniqueid[:-1] + "0"
    for key, obj in bridgeConfig["sensors"].items():
        if obj.uniqueid == lightSensorUID:
            return obj

def findTempSensor(sensor):
    lightSensorUID = sensor.uniqueid[:-1] + "2"
    for key, obj in bridgeConfig["sensors"].items():
        if obj.uniqueid == lightSensorUID:
            return obj

def convertHexToMac(hexValue):
    s = '{0:016x}'.format(int(hexValue,16))
    s = ':'.join(s[i:i + 2] for i in range(0, 16, 2))
    return s

# on_connect handler (linked to client below)

def on_disconnect(client, userdata, rc):
    """
    MQTT transport loss means diyHue can no longer verify Z2M-backed
    devices, so report them unavailable immediately.
    """
    global mqttBridgeOnline

    mqttBridgeOnline = False

    logging.warning(
        "MQTT broker disconnected rc=%s",
        rc,
    )

    for _key, device in bridgeConfig["lights"].items():
        if device.protocol == "mqtt":
            _set_mqtt_reachable(
                device,
                False,
            )


def on_connect(client, userdata, flags, rc):
    logging.debug("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # Start autodetection on lights
    autodiscoveryTopic = discoveryPrefix + "/light/+/light/config" # + in topic is wildcard
    client.subscribe(autodiscoveryTopic)

    # Normal Z2M device state.
    client.subscribe("zigbee2mqtt/+")

    # Physical availability is one MQTT level deeper.
    client.subscribe("zigbee2mqtt/+/availability")

    # Connectivity of Zigbee2MQTT itself.
    client.subscribe("zigbee2mqtt/bridge/state")

    client.subscribe("zigbee2mqtt/bridge/devices")
    client.subscribe("zigbee2mqtt/bridge/log")

    # Preserve support for custom discovery topic layouts.
    for resource in ["lights", "sensors"]:
        for _key, device in bridgeConfig[resource].items():
            _subscribe_mqtt_object_topics(
                device,
                client,
            )

    # Never trust persisted YAML light state after a process/broker
    # reconnect. Query the real physical devices after retained MQTT
    # metadata has had time to settle.
    _schedule_mqtt_state_sync(
        client,
        "mqtt-connect",
    )

    _start_motionaware_radio_worker(
        client
    )

def mqttServer():

    logging.info("Starting MQTT service...")
    # ================= MQTT CLIENT Connection========================
    # Set user/password on client if supplied

    if bridgeConfig["config"]["mqtt"]["mqttUser"] != "" and bridgeConfig["config"]["mqtt"]["mqttPassword"] != "":
        client.username_pw_set(bridgeConfig["config"]["mqtt"]["mqttUser"],bridgeConfig["config"]["mqtt"]["mqttPassword"])

    if bridgeConfig["config"]["mqtt"]['discoveryPrefix'] is not None:
        discoveryPrefix = bridgeConfig["config"]["mqtt"]['discoveryPrefix']

    # defaults for TLS and certs
    if 'mqttCaCerts' not in bridgeConfig["config"]["mqtt"]:
        bridgeConfig["config"]["mqtt"]["mqttCaCerts"] = None
    if 'mqttCertfile' not in bridgeConfig["config"]["mqtt"]:
        bridgeConfig["config"]["mqtt"]["mqttCertfile"] = None
    if 'mqttKeyfile' not in bridgeConfig["config"]["mqtt"]:
        bridgeConfig["config"]["mqtt"]["mqttKeyfile"] = None
    if 'mqttTls' not in bridgeConfig["config"]["mqtt"]:
        bridgeConfig["config"]["mqtt"]["mqttTls"] = False
    if 'mqttTlsInsecure' not in bridgeConfig["config"]["mqtt"]:
        bridgeConfig["config"]["mqtt"]["mqttTlsInsecure"] = False
    # TLS set?
    if bridgeConfig["config"]["mqtt"]["mqttTls"]:
        mqttTlsVersion = ssl.PROTOCOL_TLS
        client.tls_set(ca_certs=bridgeConfig["config"]["mqtt"]["mqttCaCerts"], certfile=bridgeConfig["config"]["mqtt"]["mqttCertfile"], keyfile=bridgeConfig["config"]["mqtt"]["mqttKeyfile"], tls_version=mqttTlsVersion)
        # allow insecure
        if bridgeConfig["config"]["mqtt"]["mqttTlsInsecure"]:
            client.tls_insecure_set(bridgeConfig["config"]["mqtt"]["mqttTlsInsecure"])
    # Setup handlers
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    # Connect to the server
    client.connect(bridgeConfig["config"]["mqtt"]["mqttServer"], bridgeConfig["config"]["mqtt"]["mqttPort"])

    # start the loop to keep receiving data
    client.loop_forever()
