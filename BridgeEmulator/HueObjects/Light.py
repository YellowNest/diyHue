import uuid
import logManager
from lights.light_types import lightTypes, archetype
from lights.protocols import protocols, wled
from HueObjects import genV2Uuid, incProcess, v1StateToV2, generate_unique_id, v2StateToV1, StreamEvent
from datetime import datetime, timezone
from copy import deepcopy
from time import sleep

logging = logManager.logger.get_logger(__name__)

class Light():
    def __init__(self, data):
        self.name = data["name"]
        self.modelid = data["modelid"]
        self.id_v1 = data["id_v1"]
        self.id_v2 = data["id_v2"] if "id_v2" in data else genV2Uuid()
        self.uniqueid = data["uniqueid"] if "uniqueid" in data else generate_unique_id()
        self.state = data["state"] if "state" in data else deepcopy(
            lightTypes[self.modelid]["state"])
        self.protocol = data["protocol"] if "protocol" in data else "dummy"
        self.config = data["config"] if "config" in data else deepcopy(
            lightTypes[self.modelid]["config"])
        self.protocol_cfg = data["protocol_cfg"] if "protocol_cfg" in data else {}
        self.streaming = False
        self.dynamics = deepcopy(lightTypes[self.modelid]["dynamics"])
        self.effect = "no_effect"
        self.function = data["function"] if "function" in data else "mixed"

        # entertainment
        streamMessage = {"creationtime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": str(uuid.uuid5(
                             uuid.NAMESPACE_URL, self.id_v2 + 'entertainment')), "type": "entertainent"}],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        streamMessage["data"][0].update(self.getV2Entertainment())
        StreamEvent(streamMessage)

        # zigbee_connectivity
        streamMessage = {"creationtime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getZigBee()],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        StreamEvent(streamMessage)

        # light
        streamMessage = {"creationtime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getV2Api()],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        StreamEvent(streamMessage)

        # device
        streamMessage = {"creationtime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getDevice()],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        streamMessage["data"][0].update(self.getDevice())
        StreamEvent(streamMessage)

    def __del__(self):
        ## light ##
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.id_v2, "type": "light"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        ## device ##
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.getDevice()["id"], "type": "device"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        # Zigbee Connectivity
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.getZigBee()["id"], "type": "zigbee_connectivity"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        # Entertainment
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.getV2Entertainment()["id"], "type": "entertainment"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        logging.info(self.name + " light was destroyed.")

    def update_attr(self, newdata):
        for key, value in newdata.items():
            updateAttribute = getattr(self, key)
            if isinstance(updateAttribute, dict):
                updateAttribute.update(value)
                setattr(self, key, updateAttribute)
            else:
                setattr(self, key, value)
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getDevice()],
                         "id": str(uuid.uuid4()),
                         "type": "update"
                         }
        StreamEvent(streamMessage)

    def getV1Api(self):
        result = lightTypes[self.modelid]["v1_static"]
        result["config"] = self.config
        result["state"] = {"on": self.state["on"]}
        if "bri" in self.state and self.modelid not in ["LOM001", "LOM004", "LOM010"]:
            result["state"]["bri"] = int(self.state["bri"]) if self.state["bri"] is not None else 1
        if "ct" in self.state and self.modelid not in ["LOM001", "LOM004", "LOM010", "LTW001", "LLC010"]:
            result["state"]["ct"] = self.state["ct"]
            result["state"]["colormode"] = self.state["colormode"]
        if "xy" in self.state and self.modelid not in ["LOM001", "LOM004", "LOM010", "LTW001", "LWB010"]:
            result["state"]["xy"] = self.state["xy"]
            result["state"]["hue"] = self.state["hue"]
            result["state"]["sat"] = self.state["sat"]
            result["state"]["colormode"] = self.state["colormode"]
        result["state"]["alert"] = self.state["alert"]
        if "mode" in self.state:
            result["state"]["mode"] = self.state["mode"]
        result["state"]["reachable"] = self.state["reachable"]
        result["modelid"] = self.modelid
        result["name"] = self.name
        result["uniqueid"] = self.uniqueid
        return result

    def updateLightState(self, state):

        if "xy" in state and "xy" in self.state:
            self.state["colormode"] = "xy"
        elif "ct" in state and "ct" in self.state:
            self.state["colormode"] = "ct"
        elif ("hue" in state or "sat" in state) and "hue" in self.state:
            self.state["colormode"] = "hs"

    def setV1State(self, state, advertise=True):
        if "lights" not in state:

            # OFF must terminate a dynamic scene regardless of
            # whether the command arrived through V1, V2 or a group.
            # Otherwise the scene worker can send another colour/
            # brightness command after OFF and wake the lamp again.
            if (
                state.get("on") is False
                and self.dynamics["status"] == "dynamic_palette"
            ):
                self.dynamics["status"] = "none"

            state = incProcess(self.state, state)
            self.updateLightState(state)
            for key, value in state.items():
                if key in self.state:
                    self.state[key] = value
                if key in self.config:
                    if key == "archetype":
                        self.config[key] = value.replace("_","")
                    else:
                        self.config[key] = value
                if key == "name":
                    self.name = value
                if key == "function":
                    self.function = value
            if "bri" in state:
                if "min_bri" in self.protocol_cfg and self.protocol_cfg["min_bri"] > state["bri"]:
                    state["bri"] = self.protocol_cfg["min_bri"]
                if "max_bri" in self.protocol_cfg and self.protocol_cfg["max_bri"] < state["bri"]:
                    state["bri"] = self.protocol_cfg["max_bri"]

        if self.protocol not in ["dummy"]:
            for protocol in protocols:
                if "lights.protocols." + self.protocol == protocol.__name__:
                    try:
                        protocol.set_light(self, state)
                        self.state["reachable"] = True
                    except Exception as e:
                        self.state["reachable"] = False
                        logging.warning(self.name + " light error, details: %s", e)
                    return
        if advertise:
            v2State = v1StateToV2(state)
            self.genStreamEvent(v2State)

    def setV2State(self, state):
        v1State = v2StateToV1(state)

        # Native Hue behaviour: manually switching the light off
        # terminates a running dynamic scene for this light.
        if (
            "on" in state
            and state["on"].get("on") is False
            and self.dynamics["status"] == "dynamic_palette"
        ):
            self.dynamics["status"] = "none"
        if "effects_v2" in state and "action" in state["effects_v2"]:
            v1State["effect"] = state["effects_v2"]["action"]["effect"]
            self.effect = v1State["effect"]
        if "effects" in state:
            v1State["effect"] = state["effects"]["effect"]
            self.effect = v1State["effect"]
        if "dynamics" in state and "speed" in state["dynamics"]:
            self.dynamics["speed"] = state["dynamics"]["speed"]
        if "metadata" in state:
            if "archetype" in state["metadata"]:
                v1State["archetype"] = state["metadata"]["archetype"]
            if "name" in state["metadata"]:
                v1State["name"] = state["metadata"]["name"]
            if "function" in state["metadata"]:
                v1State["function"] = state["metadata"]["function"]
        self.setV1State(v1State, advertise=False)
        self.genStreamEvent(state)

    def genStreamEvent(self, v2State):
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.id_v2,"id_v1": "/lights/" + self.id_v1, "type": "light"}],
                         "id": str(uuid.uuid4()),
                         "type": "update"
                         }
        streamMessage["data"][0].update(v2State)
        streamMessage["data"][0].update({"owner": {"rid": self.getDevice()["id"], "rtype": "device"}})
        streamMessage["data"][0].update({"service_id": self.protocol_cfg["light_nr"]-1 if "light_nr" in self.protocol_cfg else 0})
        StreamEvent(streamMessage)

        # Normal light state changes (on/off, dimming, colour,
        # temperature, effects, dynamics) belong to the light
        # resource only.
        #
        # Sending the complete device resource after every MQTT
        # brightness report creates a large amount of redundant SSE
        # traffic and can overwhelm Hue clients.
        #
        # Device metadata still needs its own update when metadata
        # actually changes.
        if "metadata" in v2State:
            deviceMessage = {
                "creationtime": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "data": [self.getDevice()],
                "id": str(uuid.uuid4()),
                "type": "update"
            }
            StreamEvent(deviceMessage)

    def getDevice(self):
        result = {"id": str(uuid.uuid5(
            uuid.NAMESPACE_URL, self.id_v2 + 'device'))}
        result["id_v1"] = "/lights/" + self.id_v1
        result["identify"] = {}
        result["metadata"] = {
            "archetype": archetype[self.config["archetype"]],
            "name": self.name
        }
        result["product_data"] = lightTypes[self.modelid]["device"]
        result["product_data"]["model_id"] = self.modelid
        result["service_id"] = self.protocol_cfg["light_nr"]-1 if "light_nr" in self.protocol_cfg else 0
        result["services"] = [
            {
                "rid": self.id_v2,
                "rtype": "light"
            },
            {
                "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.id_v2 + 'zigbee_connectivity')),
                "rtype": "zigbee_connectivity"
            },
            {
                "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.id_v2 + 'entertainment')),
                "rtype": "entertainment"
            }
        ]
        result["type"] = "device"
        return result

    def getZigBee(self):
        result = {}
        result["id"] = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                      self.id_v2 + 'zigbee_connectivity'))
        result["id_v1"] = "/lights/" + self.id_v1
        result["mac_address"] = self.uniqueid[:23]
        result["owner"] = {
            "rid": self.getDevice()["id"],
            "rtype": "device"
        }
        result["status"] = "connected" if self.state["reachable"] else "connectivity_issue"
        result["type"] = "zigbee_connectivity"
        return result

    def getBridgeHome(self):
        return {
            "rid": self.id_v2,
            "rtype": "light"
        }

    def getV2Api(self):
        result = {}
        result["alert"] = {"action_values": ["breathe"]}
        # gradient lights
        if self.modelid in ["LCX002", "915005987201", "LCX004", "LCX006"]:
            result["effects"] = {
                "effect_values": [
                    "no_effect",
                    "candle",
                    "fire"
                ],
                "status": self.effect,
                "status_values": [
                    "no_effect",
                    "candle",
                    "fire"
                ]
            }
            result["gradient"] = {"points": self.state["gradient"]["points"],
                                  "points_capable": self.protocol_cfg["points_capable"]}

        # color lights only
        if self.modelid in ["LST002", "LCT001", "LCT015", "LCX002", "915005987201", "LCX004", "LCX006", "LCA005", "LLC010", "LCG001"]:
            colorgamut = lightTypes[self.modelid]["v1_static"]["capabilities"]["control"]["colorgamut"]
            result["color"] = {
                "gamut": {
                    "blue":  {"x": colorgamut[2][0], "y": colorgamut[2][1]},
                    "green": {"x": colorgamut[1][0], "y": colorgamut[1][1]},
                    "red":   {"x": colorgamut[0][0], "y": colorgamut[0][1]}
                },
                "gamut_type": lightTypes[self.modelid]["v1_static"]["capabilities"]["control"]["colorgamuttype"],
                "xy": {
                    "x": self.state["xy"][0],
                    "y": self.state["xy"][1]
                }
            }
        if "ct" in self.state:
            result["color_temperature"] = {
                "mirek": self.state["ct"] if self.state["colormode"] == "ct" else None,
                "mirek_schema": {
                    "mirek_maximum": 500,
                    "mirek_minimum": 153
                }
            }
            result["color_temperature"]["mirek_valid"] = True if self.state[
                "ct"] != None and self.state["ct"] < 500 and self.state["ct"] > 153 else False
            result["color_temperature_delta"] = {}
        if "bri" in self.state:
            bri_value = self.state["bri"]
            if bri_value is None or bri_value == "null":
                bri_value = 1
            result["dimming"] = {
                "brightness": round(float(bri_value) / 2.54, 2),
                "min_dim_level": 0.1  # Adjust this value as needed
            }
            result["dimming_delta"] = {}
        result["dynamics"] = self.dynamics
        result["effects"] = {
            "effect_values": [
                "no_effect",
                "candle",
                "fire",
                "colorloop"
            ],
            "status": self.effect,
            "status_values": [
                "no_effect",
                "candle",
                "fire",
                "colorloop"
            ]
        }
        if self.protocol == "wled":
            # "none" is a v1-only internal alias for "no_effect"; "sunbeam" is a
            # silent input alias for "sunrise" (see wled.py) -- neither is advertised.
            wled_effect_values = [e for e in wled.HUE_EFFECT_TO_WLED_FX if e not in ("none", "sunbeam")]
            result["effects_v2"] = {
                "action": {"effect_values": wled_effect_values},
                "status": {
                    "effect": self.effect,
                    "effect_values": wled_effect_values
                }
            }
        result["timed_effects"] = {}
        result["identify"] = {}
        result["id"] = self.id_v2
        result["id_v1"] = "/lights/" + self.id_v1
        result["metadata"] = {"name": self.name, "function": self.function,
                              "archetype": archetype[self.config["archetype"]]}
        result["mode"] = "normal"
        if "mode" in self.state and self.state["mode"] == "streaming":
            result["mode"] = "streaming"
        result["on"] = {
            "on": self.state["on"]
        }
        result["owner"] = {
            "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.id_v2 + 'device')),
            "rtype": "device"
        }
        result["product_data"] = {"function": "mixed"}
        result["signaling"] = {"signal_values": [
            "no_signal",
            "on_off"]}
        result["powerup"] = {
            "preset": "last_on_state",
            "configured": True,
            "on": {
                 "mode": "on",
                 "on": {
                      "on": True
                }
            },
            "dimming": {
                "mode": "previous"
            }
        }
        result["service_id"] = self.protocol_cfg["light_nr"]-1 if "light_nr" in self.protocol_cfg else 0
        result["type"] = "light"
        return result

    def getV2Entertainment(self):
        entertainmenUuid = str(uuid.uuid5(
            uuid.NAMESPACE_URL, self.id_v2 + 'entertainment'))
        result = {
            "equalizer": True,
            "id": entertainmenUuid,
            "id_v1": "/lights/" + self.id_v1,
            "proxy": lightTypes[self.modelid]["v1_static"]["capabilities"]["streaming"]["proxy"],
            "renderer": lightTypes[self.modelid]["v1_static"]["capabilities"]["streaming"]["renderer"],
            "renderer_reference": {
                "rid": self.id_v2,
                "rtype": "light"
            }
        }
        result["owner"] = {
            "rid": self.getDevice()["id"], "rtype": "device"}
        result["segments"] = {
            "configurable": False
        }
        if self.modelid == "LCX002":
            result["segments"]["max_segments"] = 7
            result["segments"]["segments"] = [
                {
                    "length": 2,
                    "start": 0
                },
                {
                    "length": 2,
                    "start": 2
                },
                {
                    "length": 4,
                    "start": 4
                },
                {
                    "length": 4,
                    "start": 8
                },
                {
                    "length": 4,
                    "start": 12
                },
                {
                    "length": 2,
                    "start": 16
                },
                {
                    "length": 2,
                    "start": 18
                }]
        elif self.modelid in ["915005987201", "LCX004", "LCX006"]:
            result["segments"]["max_segments"] = 10
            result["segments"]["segments"] = [
                {
                    "length": 3,
                    "start": 0
                },
                {
                    "length": 4,
                    "start": 3
                },
                {
                    "length": 3,
                    "start": 7
                }
            ]
        else:
            result["segments"]["max_segments"] = 1
            result["segments"]["segments"] = [{
                "length": 1,
                "start": 0
            }]
        result["type"] = "entertainment"
        return result

    def getObjectPath(self):
        return {"resource": "lights", "id": self.id_v1}

    def dynamicScenePlay(self, palette, index):
        logging.debug("Start Dynamic scene play for " + self.name)
        if "dynamic_palette" in self.dynamics["status_values"]:
            self.dynamics["status"] = "dynamic_palette"
        while (
            self.dynamics["status"] == "dynamic_palette"
            and self.state.get("on", True)
        ):
            try:
                current_speed = float(self.dynamics["speed"])
            except (TypeError, ValueError):
                current_speed = 0.6269841194152832

            # diyHue's existing Hue-like timing model.
            # Avoid division by zero but otherwise preserve Hue's
            # incoming dynamics.speed value unchanged.
            current_speed = max(0.01, current_speed)
            transition = max(1, int(30 / current_speed))

            logging.debug(
                "Dynamic scene %s speed %.4f transition %.1fs",
                self.name,
                current_speed,
                transition / 10
            )
            if self.modelid in ["LCT001", "LCT015", "LST002", "LCX002", "915005987201", "LCX004", "LCX006", "LCA005"]:
                if index == len(palette["color"]):
                    index = 0
                points = []
                if self.modelid in ["LCX002", "915005987201", "LCX004", "LCX006"]:
                    # for gradient lights
                    gradientIndex = index
                    for x in range(self.protocol_cfg["points_capable"]):
                        points.append(palette["color"][gradientIndex])
                        gradientIndex += 1
                        if gradientIndex == len(palette["color"]):
                            gradientIndex = 0
                    self.setV2State(
                        {"gradient": {"points": points}, "transitiontime": transition})
                else:
                    lightState = palette["color"][index]
                    # based on youtube videos, the transition is slow
                    lightState["transitiontime"] = transition
                    self.setV2State(lightState)
            elif self.modelid == "LTW001":
                if index == len(palette["color_temperature"]):
                    index = 0
                lightState = palette["color_temperature"][index]
                lightState["transitiontime"] = transition
                self.setV2State(lightState)
            else:
                if index == len(palette["dimming"]):
                    index = 0
                lightState = palette["dimming"][index]
                lightState["transitiontime"] = transition
                self.setV2State(lightState)
            # Wait in short slices instead of one long sleep.
            # This lets Hue speed changes and OFF commands take effect
            # immediately instead of after the current transition.
            remaining = transition / 10

            while (
                remaining > 0
                and self.dynamics["status"] == "dynamic_palette"
                and self.state.get("on", True)
            ):
                try:
                    new_speed = max(
                        0.01,
                        float(self.dynamics["speed"])
                    )
                except (TypeError, ValueError):
                    new_speed = current_speed

                # Hue changed the live speed: recalculate immediately.
                if abs(new_speed - current_speed) > 0.0001:
                    break

                chunk = min(0.1, remaining)
                sleep(chunk)
                remaining -= chunk

            # OFF or scene deactivation means the worker is finished.
            if (
                self.dynamics["status"] != "dynamic_palette"
                or not self.state.get("on", True)
            ):
                self.dynamics["status"] = "none"
                break

            index += 1
            logging.debug("Step forward dynamic scene " + self.name)
        logging.debug("Dynamic Scene " + self.name + " stopped.")

    def save(self):
        result = {"id_v2": self.id_v2, "name": self.name, "modelid": self.modelid, "uniqueid": self.uniqueid, "function": self.function,
                  "state": self.state, "config": self.config, "protocol": self.protocol, "protocol_cfg": self.protocol_cfg}
        return result
