import configManager
import logManager
import os
from HueObjects import Group, EntertainmentConfiguration, Scene, BehaviorInstance, GeofenceClient, SmartScene, StreamEvent
import uuid
import json
import weakref
from flask_restful import Resource
from flask import request
from services.entertainment import entertainmentService
from threading import Thread
from time import sleep
from functions.core import nextFreeId
from datetime import datetime, timezone
from functions.scripts import behaviorScripts
from lights.discover import scanForLights
from functions.daylightSensor import daylightSensor
from functions.motionAware import (
    v2MotionAwareResources,
    v2MotionAwareRooms,
    v2MotionAreaCandidateService,
    findMotionAwareResource,
    createMotionAwareArea,
    updateMotionAwareResource,
    deleteMotionAwareArea,
    setMotionAwareDebugMotion,
)

logging = logManager.logger.get_logger(__name__)

bridgeConfig = configManager.bridgeConfig.yaml_config

# Temporary, credential-free MotionAware diagnostics.  Kept behind an
# environment switch so it can be removed/disabled without changing API data.
def _motion_diag(resource, resourceid=None, method=None, body=None, status=None):
    if os.getenv("HUE_MOTION_DIAGNOSTICS", "1").lower() not in ("1", "true", "yes"):
        return
    if resource not in PRO_MOTION_DIAGNOSTIC_TYPES and resource not in ("device", "light"):
        return
    safe = body
    if isinstance(safe, dict):
        safe = {k: ("<redacted>" if k.lower() in {"username", "clientkey", "hue-application-key", "authorization", "token"} else v) for k, v in safe.items()}
    logging.info("MOTION_DIAG %s", json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "client": request.remote_addr, "method": method or request.method, "path": request.path, "resource": resource, "resourceid": resourceid, "body": safe, "status": status}, sort_keys=True, default=str))

PRO_MOTION_RESOURCE_TYPES = (
    "motion_area_configuration",
    "convenience_area_motion",
    "security_area_motion",
)

# Diagnostics must observe MotionAware's time-slot editor too, but behavior
# instances are regular CLIP resources, not MotionAware served collections.
# Keep observability separate from resource dispatch: conflating the two made
# GET /behavior_instance/{rid} fall into findMotionAwareResource() and return
# a false empty result.
PRO_MOTION_DIAGNOSTIC_TYPES = (
    *PRO_MOTION_RESOURCE_TYPES,
    "behavior_instance",
)

# Resource types exposed by a live Bridge Pro clip capability.
# motion_area_candidate is intentionally NOT a served collection;
# it exists as a ResourceIdentifier/service on compatible devices.
PRO_CAPABILITY_RESOURCES = [
    "motion_area_configuration",
    "convenience_area_motion",
    "security_area_motion",
    "entertainment_configuration",
    "bridge",
    "button",
    "device",
    "device_power",
    "device_software_update",
    "entertainment",
    "light",
    "light_level",
    "zigbee_connectivity",
    "zgp_connectivity",
    "motion",
    "camera_motion",
    "relative_rotary",
    "temperature",
    "zigbee_device_discovery",
    "contact",
    "tamper",
    "speaker",
    "bell_button",
    "switch_input_configuration",
    "bridge_home",
    "grouped_light",
    "grouped_light_level",
    "grouped_motion",
    "room",
    "service_group",
    "zone",
    "scene",
    "matter",
    "matter_fabric",
    "behavior_script",
    "behavior_instance",
    "geofence_client",
    "geolocation",
    "smart_scene",
    "clip",
]

# We advertise these because Bridge Pro exposes them, but diyHue does
# not emulate their contents yet. Return a valid empty collection.
PRO_EMPTY_RESOURCE_TYPES = (
    "device_software_update",
    "zgp_connectivity",
    "camera_motion",
    "tamper",
    "speaker",
    "bell_button",
    "switch_input_configuration",
    "grouped_light_level",
    "grouped_motion",
    "service_group",
    "matter",
    "matter_fabric",
)

def _isBridgePro():
    return os.getenv("BRIDGE_PROFILE", "").lower() == "pro"

v2Resources = {"light": {}, "scene": {}, "smart_scene": {}, "grouped_light": {}, "room": {}, "zone": {
}, "entertainment": {}, "entertainment_configuration": {}, "zigbee_connectivity": {}, "zigbee_device_discovery": {}, "device": {}, "device_power": {},
"geofence_client": {}, "motion": {}, "light_level": {}, "temperature": {}, "relative_rotary": {}, "button": {}, "contact": {}}


def _identifyMotionAwareLight(light):
    """Map V2 device identify to a transient physical lamp action."""
    if hasattr(light, "firstElement"):
        light = light.firstElement()
    if light is None:
        logging.warning("MOTION_IDENTIFY ignored: device has no light service")
        return False
    sent = light.sendIdentifyCommand()
    logging.info(
        "MOTION_IDENTIFY light=%s protocol=%s action=%s sent=%s",
        light.id_v1,
        light.protocol,
        {"identify": "transient"},
        sent,
    )
    return sent


def _withMotionAreaCandidate(device):
    """Attach the candidate service reference owned by this device."""
    if not isinstance(device, dict):
        return device
    owner_id = device.get("id")
    for _group, candidates in v2MotionAwareRooms():
        for candidate in candidates:
            if candidate.getDevice().get("id") != owner_id:
                continue
            services = device.setdefault("services", [])
            reference = v2MotionAreaCandidateService(candidate)
            if reference not in services:
                services.append(reference)
            return device
    return device


def _isVisibleBehaviorInstance(instance):
    """Return false only for a behavior bound to a deleted MotionAware area.

    Hue keeps behavior instances in its local resource cache.  Returning an
    instance whose source area no longer exists makes the client treat the
    associated room/area as an externally removed or malformed resource.  Do
    not apply this rule to normal automation behavior instances.
    """
    configuration = getattr(instance, "configuration", None)
    if not isinstance(configuration, dict):
        return True

    source = configuration.get("source")
    if (
        not isinstance(source, dict)
        or source.get("rtype") != "motion_area_configuration"
    ):
        return True

    return findMotionAwareResource(
        "motion_area_configuration", source.get("rid")
    ) is not None


def getObject(element, v2uuid):
    if element in ["behavior_instance"]:
        return bridgeConfig[element][v2uuid]
    elif element in v2Resources and v2uuid in v2Resources[element]:
        logging.debug("Cache Hit for " + element)
        return v2Resources[element][v2uuid]()
    elif element in ["light", "scene", "grouped_light", "smart_scene"]:
        for v1Element in ["lights", "groups", "scenes", "smart_scene"]:
            for key, obj in bridgeConfig[v1Element].items():
                if obj.id_v2 == v2uuid:
                    v2Resources[element][v2uuid] = weakref.ref(obj)
                    logging.debug("Cache Miss " + element)
                    return obj
    elif element in ["entertainment"]:
        for key, obj in bridgeConfig["lights"].items():
            if str(uuid.uuid5(uuid.NAMESPACE_URL, obj.id_v2 + 'entertainment')) == v2uuid:
                v2Resources[element][v2uuid] = weakref.ref(obj)
                return obj
    else:
        for v1Element in ["lights", "groups", "scenes", "sensors", "geofence_clients"]:
            for key, obj in bridgeConfig[v1Element].items():
                if str(uuid.uuid5(uuid.NAMESPACE_URL, obj.id_v2 + element)) == v2uuid:
                    logging.debug("Cache Miss " + element)
                    v2Resources[element][v2uuid] = weakref.ref(obj)
                    return obj
                elif obj.id_v2 == v2uuid:
                    logging.debug("Cache Miss " + element)
                    v2Resources[element][v2uuid] = weakref.ref(obj)
                    return obj
    logging.info("element not found!")
    return False


def authorizeV2(headers):
    if "hue-application-key" in headers and headers["hue-application-key"] in bridgeConfig["apiUsers"]:
        bridgeConfig["apiUsers"][headers["hue-application-key"]
                                 ].last_use_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        return {"user": bridgeConfig["apiUsers"][headers["hue-application-key"]]}
    return []

def v2BridgeEntertainment():
    return {"id": "57a9ebc9-406d-4a29-a4ff-42acee9e9be7",
            "owner": {
                "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'device')),
                "rtype": "device"
                },
            "renderer": False,
            "proxy": True,
            "equalizer": False,
            "max_streams": 1,
            "type": "entertainment"
            }



def v2HomeKit():
    return {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'homekit')),
            "status": "unpaired",
            "status_values": [
                "pairing",
                "paired",
                "unpaired"
            ],
        "type": "homekit"
    }


def v2BridgeZigBee():
    return {"id": str(uuid.uuid5(
        uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'zigbee_connectivity')),
            "owner": {
                "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'device')),
                "rtype": "device"
                },
            "status": "connected",
            "mac_address": bridgeConfig["config"]["mac"][:8] + ":01:01:" +  bridgeConfig["config"]["mac"][9:],
            "channel": {
                "value": "channel_25",
                "status": "set"
                },
            "type": "zigbee_connectivity"
            }

def v2BridgeZigBeeDiscovery():
    return{"id": str(uuid.uuid5(
        uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'zigbee_device_discovery')),
        "owner": {
            "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'device')),
            "rtype": "device"
        },
        "action": {
            "action_type_values": [
                "search"
            ]
        },
        "status": bridgeConfig["config"]["zigbee_device_discovery_info"]["status"],
        "type": "zigbee_device_discovery",
        }


def v2GeofenceClient():
    user = authorizeV2(request.headers)
    result = {
      "id": str(uuid.uuid5(uuid.NAMESPACE_URL, request.headers["hue-application-key"])),
      "name": user["user"].name,
      "type": "geofence_client"
    }
    return result


def v2BridgeHomeId():
    """Return the persistent CLIP id for the single bridge-home resource.

    Group zero is an in-memory diyHue helper and is recreated at startup.  It
    must not define a public CLIP identifier, because persisted V2 references
    (including MotionAware groups) would otherwise become dangling after each
    bridge restart.
    """
    return str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        bridgeConfig["config"]["bridgeid"] + "bridge_home"
    ))


def v2BridgeHome():
    result = {}
    result["children"] = []
    result["children"].append({"rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'device')), "rtype": "device"})
    #result["grouped_services"] = []
    #if len(bridgeConfig["lights"]) > 0:
    #    result["grouped_services"].append({
    #        "rid": bridgeConfig["groups"]["0"].id_v2,
    #        "rtype": "grouped_light"
    #    })
    result["id"] = v2BridgeHomeId()
    result["id_v1"] = "/groups/0"
    result["services"] = []
    result["type"] = "bridge_home"
    for key, light in bridgeConfig["lights"].items():
        result["services"].append(light.getBridgeHome())
        result["children"].append({"rid": light.getDevice()["id"], "rtype": "device"})
    for key, group in bridgeConfig["groups"].items():
        if group.type == "Room":
            result["children"].append({"rid": group.getV2Room()["id"], "rtype": "room"})
    for key, sensor in bridgeConfig["sensors"].items():
        device = sensor.getDevice()

        if device is None:
            continue

        if (
            sensor.modelid == "DS01"
            and sensor.type == "ZHAOpenClose"
        ):
            child = {
                "rid": device["id"],
                "rtype": "device"
            }

            if child not in result["children"]:
                result["children"].append(child)

        elif sensor.getBridgeHome():
            result["services"].append(
                sensor.getBridgeHome()
            )
    result["services"].append({"rid": bridgeConfig["groups"]["0"].id_v2 ,"rtype": "grouped_light"})
    return result


def v2Bridge():
    bridge_id = bridgeConfig["config"]["bridgeid"]
    return {
        "bridge_id": bridge_id.lower(),
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'bridge')),
        "id_v1": "",
        "owner": {"rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'device')), "rtype": "device"},
        "time_zone": {"time_zone": bridgeConfig["config"]["timezone"]},
        "type": "bridge"
    }

def geoLocation():
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, bridgeConfig["config"]["bridgeid"] + 'geolocation')),
        "is_configured": bridgeConfig["sensors"]["1"].config["configured"],
        "sun_today": {
            "sunset_time": bridgeConfig["sensors"]["1"].config["sunset"] if "lat" in bridgeConfig["sensors"]["1"].protocol_cfg else "",
            "day_type": "normal_day"
        },
        "type": "geolocation"
    }



def v2Clip():
    if not _isBridgePro():
        return None

    config = bridgeConfig["config"]
    return {
        "id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            config["bridgeid"] + "clip"
        )),
        "resources": PRO_CAPABILITY_RESOURCES.copy(),
        "type": "clip"
    }


def v2BridgeDevice():
    config = bridgeConfig["config"]
    bridge_id = config["bridgeid"]
    result = {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'device')), "type": "device"}
    result["id_v1"] = ""
    bridge_pro = os.getenv("BRIDGE_PROFILE", "").lower() == "pro"
    archetype = "bridge_v3" if bridge_pro else "bridge_v2"
    result["metadata"] = {"archetype": archetype, "name": config["name"]}
    result["identify"] = {}
    result["product_data"] = {
        "certified": True,
        "manufacturer_name": "Signify Netherlands B.V.",
        "model_id": "BSB003" if bridge_pro else "BSB002",
        "product_archetype": archetype,
        "product_name": "Hue Bridge" if bridge_pro else "Philips hue",
        "software_version": "1.78.2071442000" if bridge_pro else config["apiversion"][:5] + config["swversion"]
    }
    result["services"] = [
        {"rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'bridge')), "rtype": "bridge"},
        {"rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'zigbee_connectivity')), "rtype": "zigbee_connectivity"},
        {"rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'zigbee_device_discovery')), "rtype": "zigbee_device_discovery"},
        {"rid": "57a9ebc9-406d-4a29-a4ff-42acee9e9be7", "rtype": "entertainment"}
    ]
    return result

def v2DiyHueBridge():
    bridge_id = bridgeConfig["config"]["bridgeid"]
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'diyhue')),
        "id_v1": "",
        "owner": {"rid": str(uuid.uuid5(uuid.NAMESPACE_URL, bridge_id + 'device')), "rtype": "device"},
        "type": "diyhue",
        "hue_essentials_key": bridgeConfig["config"]["Hue Essentials key"], 
        "remote_api_enabled": bridgeConfig["config"]["Remote API enabled"],
        "remote_discovery": bridgeConfig["config"]["discovery"]
    }

class AuthV1(Resource):
    def get(self):
        authorisation = authorizeV2(request.headers)
        if "user" in authorisation:
            logging.debug("Auth 200")
            return {}, 200, {'hue-application-id': request.headers["hue-application-key"]}

        else:
            logging.info("Auth 403")
            return '', 403


class ClipV2(Resource):
    def get(self):
        authorisation = authorizeV2(request.headers)
        if "user" not in authorisation:
            return "", 403
        data = []
        # homekit
        data.append(v2HomeKit())
        # device
        data.append(v2BridgeDevice())
        for key, light in bridgeConfig["lights"].items():
            data.append(_withMotionAreaCandidate(light.getDevice()))
        for key, sensor in bridgeConfig["sensors"].items():
            if sensor.getDevice() != None:
                data.append(sensor.getDevice())
        # bridge
        data.append(v2Bridge())
        data.append(v2DiyHueBridge())

        clip = v2Clip()
        if clip is not None:
            data.append(clip)
        # zigbee
        data.append(v2BridgeZigBee())
        for key, light in bridgeConfig["lights"].items():
            data.append(light.getZigBee())
        for key, sensor in bridgeConfig["sensors"].items():
            if (
                sensor.getDevice() is not None
                and sensor.getZigBee() is not None
            ):
                data.append(
                    sensor.getZigBee()
                )
        data.append(v2BridgeZigBeeDiscovery())
        # entertainment
        data.append(v2BridgeEntertainment())
        for key, light in bridgeConfig["lights"].items():
            data.append(light.getV2Entertainment())
        # scenes
        for key, scene in bridgeConfig["scenes"].items():
            data.append(scene.getV2Api())
        # smart_scene
        for key, smartscene in bridgeConfig["smart_scene"].items():
            data.append(smartscene.getV2Api())
        # lights
        for key, light in bridgeConfig["lights"].items():
            data.append(light.getV2Api())
        # room
        for key, group in bridgeConfig["groups"].items():
            if group.type == "Room":
                data.append(group.getV2Room())
            elif group.type == "Zone":
                data.append(group.getV2Zone())
        # behavior_instance
        for key, instance in bridgeConfig["behavior_instance"].items():
            if _isVisibleBehaviorInstance(instance):
                data.append(instance.getV2Api())
        # entertainment_configuration
        for key, group in bridgeConfig["groups"].items():
            if group.type == "Entertainment":
                data.append(group.getV2Api())
        # group
            else:
                data.append(group.getV2GroupedLight())
        # bridge home
        data.append(v2BridgeHome())
        data.append(v2GeofenceClient())
        data.append(geoLocation())
        for script in behaviorScripts():
            data.append(script)
        for key, sensor in bridgeConfig["sensors"].items():
            motion = sensor.getMotion()
            if motion != None:
                data.append(motion)
            buttons = sensor.getButtons()
            if len(buttons) != 0:
                for button in buttons:
                    data.append(button)
            power = sensor.getDevicePower()
            if power != None:
                data.append(power)
            contact = sensor.getContact()
            if contact != None:
                data.append(contact)
            rotarys = sensor.getRotary()
            if len(rotarys) != 0:
                for rotary in rotarys:
                    data.append(rotary)
            temperature = sensor.getTemperature()
            if temperature != None:
                data.append(temperature)
            lightlevel = sensor.getLightlevel()
            if lightlevel != None:
                data.append(lightlevel)

        for resources in v2MotionAwareResources().values():
            data.extend(resources)

        return {"errors": [], "data": data}


class ClipV2Resource(Resource):
    def get(self, resource):
        # logging.debug(request.headers)
        authorisation = authorizeV2(request.headers)
        if "user" not in authorisation:
            return "", 403
        response = {"data": [], "errors": []}
        if resource == "scene":
            for key, scene in bridgeConfig["scenes"].items():
                response["data"].append(scene.getV2Api())
        elif resource == "smart_scene":
            for key, smartscene in bridgeConfig["smart_scene"].items():
                response["data"].append(smartscene.getV2Api())
        elif resource == "light":
            for key, light in bridgeConfig["lights"].items():
                response["data"].append(light.getV2Api())
        elif resource == "room":
            for key, group in bridgeConfig["groups"].items():
                if group.type == "Room":
                    response["data"].append(group.getV2Room())
        elif resource == "zone":
            for key, group in bridgeConfig["groups"].items():
                if group.type == "Zone":
                    response["data"].append(group.getV2Zone())
        elif resource == "grouped_light":
            for key, group in bridgeConfig["groups"].items():
                response["data"].append(group.getV2GroupedLight())
        elif resource == "zigbee_connectivity":
            for key, light in bridgeConfig["lights"].items():
                zigbee = light.getZigBee()
                if zigbee != None:
                    response["data"].append(zigbee)
            for key, sensor in bridgeConfig["sensors"].items():
                zigbee = sensor.getZigBee()
                if zigbee != None:
                    response["data"].append(zigbee)
            response["data"].append(v2BridgeZigBee())  # the bridge
        elif resource == "entertainment":
            for key, light in bridgeConfig["lights"].items():
                response["data"].append(light.getV2Entertainment())
            response["data"].append(v2BridgeEntertainment())
        elif resource == "entertainment_configuration":
            for key, group in bridgeConfig["groups"].items():
                if group.type == "Entertainment":
                    response["data"].append(group.getV2Api())
        elif resource == "device":
            for key, light in bridgeConfig["lights"].items():
                response["data"].append(_withMotionAreaCandidate(light.getDevice()))
            for key, sensor in bridgeConfig["sensors"].items():
                device = sensor.getDevice()
                if device != None:
                    response["data"].append(device)
            response["data"].append(v2BridgeDevice())  # the bridge
        elif resource == "zigbee_device_discovery":
            response["data"].append(v2BridgeZigBeeDiscovery())
        elif resource == "bridge":
            response["data"].append(v2Bridge())
        elif resource == "diyhue":
            response["data"].append(v2DiyHueBridge())
        elif resource == "bridge_home":
            response["data"].append(v2BridgeHome())
        elif resource == "homekit":
            response["data"].append(v2HomeKit())
        elif resource == "geolocation":
            response["data"].append(geoLocation())
        elif resource == "behavior_instance":
            for key, instance in bridgeConfig["behavior_instance"].items():
                if _isVisibleBehaviorInstance(instance):
                    response["data"].append(instance.getV2Api())
        elif resource == "geofence_client":
            response["data"].append(v2GeofenceClient())
        elif resource == "behavior_script":
            for script in behaviorScripts():
                response["data"].append(script)
        elif resource == "motion":
            for key, sensor in bridgeConfig["sensors"].items():
                motion = sensor.getMotion()
                if motion != None:
                    response["data"].append(motion)
        elif resource == "contact":
            for key, sensor in bridgeConfig["sensors"].items():
                contact = sensor.getContact()
                if contact != None:
                    response["data"].append(contact)
        elif resource == "device_power":
            for key, sensor in bridgeConfig["sensors"].items():
                power = sensor.getDevicePower()
                if power != None:
                    response["data"].append(power)
        elif resource == "button":
            for key, sensor in bridgeConfig["sensors"].items():
                buttons = sensor.getButtons()
                if len(buttons) != 0:
                    for button in buttons:
                        response["data"].append(button)
        elif resource == "relative_rotary":
            for key, sensor in bridgeConfig["sensors"].items():
                rotarys = sensor.getRotary()
                if len(rotarys) != 0:
                    for rotary in rotarys:
                        response["data"].append(rotary)
        elif resource == "temperature":
            for key, sensor in bridgeConfig["sensors"].items():
                temperature = sensor.getTemperature()
                if temperature != None:
                    response["data"].append(temperature)
        elif resource == "light_level":
            for key, sensor in bridgeConfig["sensors"].items():
                lightlevel = sensor.getLightlevel()
                if lightlevel != None:
                    response["data"].append(lightlevel)
        elif resource == "clip" and _isBridgePro():
            response["data"].append(v2Clip())
        elif resource in PRO_EMPTY_RESOURCE_TYPES and _isBridgePro():
            # Valid Bridge Pro collection, not implemented yet.
            pass
        elif resource in PRO_MOTION_RESOURCE_TYPES and _isBridgePro():
            response["data"].extend(v2MotionAwareResources().get(resource, []))
        else:
            response["errors"].append({"description": "Not Found"})
            del response["data"]

        return response

    def post(self, resource):
        # logging.debug(request.headers)
        authorisation = authorizeV2(request.headers)
        if "user" not in authorisation:
            return "", 403
        postDict = request.get_json(force=True)
        _motion_diag(resource, method="POST", body=postDict)
        if resource == "motion_area_configuration":
            created = createMotionAwareArea(postDict)
            if created is None:
                return "", 403
            # Hue 5.73 decodes the create result as MotionAreaConfigurationId,
            # not as a full resource. Keep this acknowledgement deliberately
            # minimal (the standard V2 create contract); the app fetches the
            # complete resource after it has the identifier.
            result = {
                "rid": created["id"],
                "rtype": resource,
            }
            _motion_diag(
                resource,
                created["id"],
                method="POST",
                body=postDict,
                status=201,
            )
            return {"data": [result], "errors": []}, 201
        logging.info(postDict)
        newObject = None
        if resource == "scene":
            new_object_id = nextFreeId(bridgeConfig, "scenes")
            objCreation = {
                "id_v1": new_object_id,
                "name": postDict["metadata"]["name"],
                "image": postDict["metadata"]["image"]["rid"] if "image" in postDict["metadata"] else None,
                "owner": bridgeConfig["apiUsers"][request.headers["hue-application-key"]],
            }
            if "group" in postDict:
                objCreation["group"] = weakref.ref(
                    getObject(postDict["group"]["rtype"], postDict["group"]["rid"]))
                objCreation["type"] = "GroupScene"
                del postDict["group"]
            elif "lights" in postDict:
                objCreation["type"] = "LightScene"
                objLights = []
                for light in postDict["lights"]:
                    objLights.append(getObject(light["rtype"], light["rid"]))
                objCreation["lights"] = objLights
            objCreation.update(postDict)
            newObject = Scene.Scene(objCreation)
            bridgeConfig["scenes"][new_object_id] = newObject
            configManager.bridgeConfig.mark_dirty("scenes")
            if "actions" in postDict:
                for action in postDict["actions"]:
                    if "target" in action:
                        if action["target"]["rtype"] == "light":
                            lightObj = getObject(
                                "light",  action["target"]["rid"])
                            sceneState = {}
                            scene = action["action"]
                            if "on" in scene:
                                sceneState["on"] = scene["on"]["on"]
                            if "dimming" in scene:
                                sceneState["bri"] = int(
                                    scene["dimming"]["brightness"] * 2.54)
                            if "color" in scene:
                                if "xy" in scene["color"]:
                                    sceneState["xy"] = [
                                        scene["color"]["xy"]["x"], scene["color"]["xy"]["y"]]
                            if "color_temperature" in scene:
                                if "mirek" in scene["color_temperature"]:
                                    sceneState["ct"] = scene["color_temperature"]["mirek"]
                            if "gradient" in scene:
                                sceneState["gradient"] = scene["gradient"]
                            newObject.lightstates[lightObj] = sceneState
            # Persist immediately so the scene survives a bridge restart.
            # The v1 REST API always saved after scene writes; the v2 path was missing this.
            configManager.bridgeConfig.save_config(backup=False, resource="scenes")
        elif resource == "smart_scene":
            new_object_id = nextFreeId(bridgeConfig, "smart_scene")
            objCreation = {
                "id_v1": new_object_id,
                "name": postDict["metadata"]["name"],
                "image": postDict["metadata"]["image"]["rid"] if "image" in postDict["metadata"] else None,
                "action": postDict["recall"]["action"],
                "timeslots": postDict["week_timeslots"][0]["timeslots"],
                "recurrence": postDict["week_timeslots"][0]["recurrence"]
            }
            del postDict["week_timeslots"]
            objCreation.update(postDict)
            newObject = SmartScene.SmartScene(objCreation)
            bridgeConfig["smart_scene"][new_object_id] = newObject
            configManager.bridgeConfig.mark_dirty("smart_scene")
        elif resource == "behavior_instance":
            newObject = BehaviorInstance.BehaviorInstance(postDict)
            bridgeConfig["behavior_instance"][newObject.id_v2] = newObject
            # Persist immediately — behavior instances define which switch buttons do what.
            # Without this save they are lost on restart, breaking all physical controls.
            configManager.bridgeConfig.save_config(backup=False, resource="behavior_instance")
        elif resource == "entertainment_configuration":
            new_object_id = nextFreeId(bridgeConfig, "groups")
            objCreation = {
                "id_v1": new_object_id,
                "name": postDict["metadata"]["name"]
            }
            objCreation.update(postDict)
            newObject = EntertainmentConfiguration.EntertainmentConfiguration(objCreation)
            if "locations" in postDict:
                if "service_locations" in postDict["locations"]:
                    for element in postDict["locations"]["service_locations"]:
                        obj = getObject(
                            element["service"]["rtype"], element["service"]["rid"])
                        newObject.add_light(obj)
                        newObject.locations[obj] = element["positions"]
            bridgeConfig["groups"][new_object_id] = newObject
            configManager.bridgeConfig.mark_dirty("groups")
            # Fire update so clients see lights/channels added after constructor's initial empty event
            streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                             "data": [newObject.getV2Api()],
                             "id": str(uuid.uuid4()),
                             "type": "update"}
            StreamEvent(streamMessage)
        elif resource in ["room", "zone"]:
            new_object_id = nextFreeId(bridgeConfig, "groups")
            objCreation = {
                "id_v1": new_object_id,
                "name": postDict["metadata"]["name"],
                "owner": bridgeConfig["apiUsers"][request.headers["hue-application-key"]],
            }
            objCreation["type"] = "Room" if resource == "room" else "Zone"
            if "archetype" in postDict["metadata"]:
                objCreation["icon_class"] = postDict["metadata"]["archetype"].replace("_", " ").capitalize()
            objCreation.update(postDict)
            newObject = Group.Group(objCreation)
            if "children" in postDict:
                for children in postDict["children"]:
                    obj = getObject(
                        children["rtype"], children["rid"])
                    newObject.add_light(obj)

            bridgeConfig["groups"][new_object_id] = newObject
            configManager.bridgeConfig.mark_dirty("groups")
        elif resource == 'geofence_client':
            new_object_id = nextFreeId(bridgeConfig, "geofence_clients")
            objCreation = {
                "id_v1": new_object_id,
                "name": postDict["name"],
                "type": "geofence_client",
                "is_at_home": postDict.get("is_at_home", False)
            }
            newObject = GeofenceClient.GeofenceClient(objCreation)
            bridgeConfig["geofence_clients"][new_object_id] = newObject
            # NOTE: geofence_clients is memory-only — no backing YAML file, no save() method
        else:
            return {
                "errors": [{
                    "description": f"Resource type not supported: {resource}"
                }]
            }, 500

        # A room/zone CLIP resource is distinct from its grouped_light service.
        # `Group.id_v2` is the latter, so returning it together with rtype
        # "room" or "zone" gives clients a dangling resource reference.  Hue
        # immediately reuses the create response when it writes a MotionAware
        # behavior's `where` target, therefore acknowledge the actual group
        # resource ID instead.
        if resource == "room":
            resource_id = newObject.getV2Room()["id"]
        elif resource == "zone":
            resource_id = newObject.getV2Zone()["id"]
        else:
            resource_id = newObject.id_v2

        # return message
        returnMessage = {"data": [{
            "rid": resource_id,
            "rtype": resource}
        ], "errors": []}

        logging.debug(json.dumps(returnMessage, sort_keys=True, indent=4))
        return returnMessage


class ClipV2ResourceId(Resource):
    def get(self, resource, resourceid):
        # logging.debug(request.headers)
        authorisation = authorizeV2(request.headers)
        if "user" not in authorisation:
            return "", 403
        if resource == "clip" and _isBridgePro():
            clip = v2Clip()
            return {
                "errors": [],
                "data": [clip] if clip and clip["id"] == resourceid else []
            }
        if resource == "bridge_home":
            # A MotionAware area may reference the bridge home as its group.
            # It is a generated singleton rather than a weakref-backed V1
            # object, so resolve it directly instead of falling into
            # getObject()/v2Resources (which returns an empty result or 500).
            home = v2BridgeHome()
            return {
                "errors": [],
                "data": [home] if home["id"] == resourceid else []
            }
        if resource in PRO_EMPTY_RESOURCE_TYPES and _isBridgePro():
            return {"errors": [], "data": []}
        if resource == "behavior_instance":
            # Hue opens a MotionAware area through its behavior instance RID.
            # The collection endpoint was implemented, but the ID endpoint
            # fell through getObject(), which has no behavior-instance lookup
            # and returned a misleading 200 with an empty data array.  That
            # is treated by the client as an externally removed rule.
            object = next(
                (
                    instance
                    for instance in bridgeConfig["behavior_instance"].values()
                    if instance.id_v2 == resourceid
                ),
                None,
            )
            return {
                "errors": [],
                "data": [object.getV2Api()]
                if object and _isVisibleBehaviorInstance(object)
                else []
            }
        if resource in PRO_MOTION_RESOURCE_TYPES and _isBridgePro():
            object = findMotionAwareResource(resource, resourceid)
            return {"errors": [], "data": [object] if object else []}
        object = getObject(resource, resourceid)
        if not object:
            return {"errors": [], "data": []}

        if resource in ["scene", "light", "smart_scene"]:
            return {"errors": [], "data": [object.getV2Api()]}
        elif resource == "room":
            return {"errors": [], "data": [object.getV2Room()]}
        elif resource == "zone":
            return {"errors": [], "data": [object.getV2Zone()]}
        elif resource == "grouped_light":
            return {"errors": [], "data": [object.getV2GroupedLight()]}
        elif resource == "device":
            return {"errors": [], "data": [object.getDevice()]}
        elif resource == "zigbee_connectivity":
            return {"errors": [], "data": [object.getZigBee()]}
        elif resource == "zigbee_device_discovery":
            return {"errors": [], "data": [object.getZigBeeDiscovery()]}
        elif resource == "entertainment":
            return {"errors": [], "data": [object.getV2Entertainment()]}
        elif resource == "entertainment_configuration":
            return {"errors": [], "data": [object.getV2Api()]}
        elif resource == "bridge":
            return {"errors": [], "data": [v2Bridge()]}
        elif resource == "motion":
            return {"errors": [], "data": [object.getMotion()]}
        elif resource == "contact":
            return {"errors": [], "data": [object.getContact()]}
        elif resource == "device_power":
            return {"errors": [], "data": [object.getDevicePower()]}
        elif resource == "button":
            return {"errors": [], "data": [object.getButtons()]}
        elif resource == "relative_rotary":
            return {"errors": [], "data": [object.getRotary()]}
        elif resource == "temperature":
            return {"errors": [], "data": [object.getTemperature()]}
        elif resource == "light_level":
            return {"errors": [], "data": [object.getLightlevel()]}

    def put(self, resource, resourceid):
        logging.debug(request.headers)
        authorisation = authorizeV2(request.headers)
        if "user" not in authorisation:
            return "", 403
        putDict = request.get_json(force=True)
        _motion_diag(resource, resourceid, method="PUT", body=putDict)

        # MOTIONAWARE_DEBUG_PUT_V1
        # Temporary private field for proving live state + SSE.
        if (
            resource in (
                "convenience_area_motion",
                "security_area_motion"
            )
            and "debug_motion" in putDict
        ):
            updated = setMotionAwareDebugMotion(
                resource,
                resourceid,
                bool(putDict["debug_motion"])
            )

            if updated is None:
                return {
                    "errors": [{
                        "description": "MotionAware resource not found"
                    }]
                }, 404

            return {
                "data": [updated],
                "errors": []
            }

        if resource in ("motion_area_configuration", "convenience_area_motion", "security_area_motion"):
            updated = updateMotionAwareResource(resource, resourceid, putDict)
            if updated is None:
                return {"errors": [{"description": "Resource not found"}]}, 404
            _motion_diag(resource, resourceid, method="PUT", body=putDict, status=200)
            return {"data": [updated], "errors": []}
        logging.info(putDict)
        object = getObject(resource, resourceid)
        if resource == "light":
            object.setV2State(putDict)
        elif resource == "entertainment_configuration":
            if "action" in putDict:
                if putDict["action"] == "start":
                    logging.info("start hue entertainment")
                    for light in object.lights:
                        light().update_attr({"state": {"mode": "streaming"}})
                    object.update_attr({"stream": {"active": True, "owner": authorisation["user"].username, "proxymode": "auto", "proxynode": "/bridge"}})
                    Thread(target=entertainmentService, args=[
                           object, authorisation["user"]]).start()
                    sleep(1)
                elif putDict["action"] == "stop":
                    logging.info("stop entertainment")
                    for light in object.lights:
                        light().update_attr({"state": {"mode": "homeautomation"}})
                    proc = object.stream.get("_proc")
                    if proc:
                        proc.kill()
                    object.update_attr({"stream": {"active": False}})
        elif resource == "scene":
            scene_changed = False

            # Hue API v2 scene editing supplies the scene's action list here.
            # This was previously ignored by diyHue, causing lamp selections
            # made in the Hue app to disappear.
            if "actions" in putDict:
                newLightStates = weakref.WeakKeyDictionary()
                actionLights = []

                for action in putDict["actions"]:
                    target = action.get(
                        "target",
                        {},
                    )

                    if target.get("rtype") != "light":
                        continue

                    targetRid = target.get("rid")

                    if not targetRid:
                        continue

                    lightObj = getObject(
                        "light",
                        targetRid,
                    )

                    if not lightObj:
                        logging.warning(
                            "Scene action references unknown light "
                            + str(targetRid)
                        )
                        continue

                    sceneAction = action.get(
                        "action",
                        {},
                    )

                    sceneState = {}

                    onState = sceneAction.get("on")

                    if (
                        isinstance(onState, dict)
                        and "on" in onState
                    ):
                        sceneState["on"] = bool(
                            onState["on"]
                        )

                    dimming = sceneAction.get(
                        "dimming"
                    )

                    if (
                        isinstance(dimming, dict)
                        and dimming.get("brightness")
                            is not None
                    ):
                        sceneState["bri"] = int(
                            max(
                                0.0,
                                min(
                                    100.0,
                                    float(
                                        dimming["brightness"]
                                    ),
                                ),
                            )
                            * 2.54
                        )

                    color = sceneAction.get(
                        "color"
                    )

                    if isinstance(color, dict):
                        xy = color.get("xy")

                        if (
                            isinstance(xy, dict)
                            and xy.get("x") is not None
                            and xy.get("y") is not None
                        ):
                            sceneState["xy"] = [
                                float(xy["x"]),
                                float(xy["y"]),
                            ]

                    colorTemperature = sceneAction.get(
                        "color_temperature"
                    )

                    if isinstance(
                        colorTemperature,
                        dict,
                    ):
                        mirek = colorTemperature.get(
                            "mirek"
                        )

                        if mirek is not None:
                            sceneState["ct"] = int(
                                mirek
                            )

                    if "gradient" in sceneAction:
                        sceneState["gradient"] = (
                            sceneAction["gradient"]
                        )

                    newLightStates[lightObj] = (
                        sceneState
                    )

                    actionLights.append(
                        lightObj
                    )

                # "actions" represents the current V2 scene action set.
                # Replacing rather than merging is important: a light that
                # disappears from the action list must no longer be triggered.
                object.lightstates = newLightStates

                # LightScene owns its explicit light list. GroupScene keeps
                # the room's complete light list and uses lightstates to
                # determine scene participation.
                if object.type == "LightScene":
                    object.lights = [
                        weakref.ref(light)
                        for light in actionLights
                    ]

                scene_changed = True

            # Preserve the existing diyHue speed compatibility fix.
            # Hue scene speed is 0.0..1.0. Apply it BEFORE recall
            # and propagate changes to an already running dynamic scene.
            if "speed" in putDict:
                try:
                    requested_speed = max(
                        0.0,
                        min(
                            1.0,
                            float(putDict["speed"]),
                        ),
                    )

                    object.speed = requested_speed

                    for light_ref in object.lights:
                        light = light_ref()

                        if (
                            light
                            and light.dynamics["status"]
                                == "dynamic_palette"
                        ):
                            # dynamicScenePlay divides by speed. Keep API
                            # speed=0 while using a safe execution floor.
                            light.dynamics["speed"] = max(
                                requested_speed,
                                0.01,
                            )

                    scene_changed = True

                except (TypeError, ValueError):
                    pass

            if "recall" in putDict:
                # Scene.activate() copies object.speed to every light.
                # Preserve API speed=0 while supplying a safe execution
                # floor to diyHue's current 30/speed implementation.
                original_speed = object.speed

                if original_speed <= 0:
                    object.speed = 0.01

                try:
                    object.activate(putDict)
                finally:
                    object.speed = original_speed

            if "palette" in putDict:
                object.palette = putDict["palette"]
                scene_changed = True

            if "metadata" in putDict:
                metadata = putDict["metadata"]

                if "name" in metadata:
                    object.name = metadata["name"]
                    scene_changed = True

                if "image" in metadata:
                    image = metadata["image"]

                    if isinstance(image, dict):
                        object.image = image.get(
                            "rid"
                        )
                    else:
                        object.image = None

                    scene_changed = True

            if scene_changed:
                object.lastupdated = datetime.now(
                    timezone.utc
                ).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )

                configManager.bridgeConfig.mark_dirty(
                    "scenes"
                )

                # Scene edits are user configuration and must survive a
                # restart immediately rather than waiting for debounce.
                configManager.bridgeConfig.save_config(
                    backup=False,
                    resource="scenes",
                )
        elif resource == "smart_scene":
            if "recall" in putDict and "action" in putDict["recall"]:
                object.activate(putDict)
            if "transition_duration" in putDict:
                object.speed = putDict["transition_duration"]
            if "week_timeslots" in putDict:
                if "timeslots" in putDict["week_timeslots"][0]:
                    object.timeslots = putDict["week_timeslots"][0]["timeslots"]
                if "recurrence" in putDict["week_timeslots"][0]:
                    object.recurrence = putDict["week_timeslots"][0]["recurrence"]
                configManager.bridgeConfig.mark_dirty("smart_scene")
            if "metadata" in putDict:
                object.name = putDict["metadata"]["name"]
                configManager.bridgeConfig.mark_dirty("smart_scene")
        elif resource == "grouped_light":
            object.setV2Action(putDict)
        elif resource == "geolocation":
            bridgeConfig["sensors"]["1"].protocol_cfg = {
                "lat": putDict["latitude"], "long": putDict["longitude"]}
            bridgeConfig["sensors"]["1"].config["configured"] = True
            configManager.bridgeConfig.mark_dirty("sensors")
            daylightSensor(bridgeConfig["config"]["timezone"], bridgeConfig["sensors"]["1"])
        elif resource == "behavior_instance":
            object.update_attr(putDict)
            # Keep the on-disk config in sync so edits survive a bridge restart.
            configManager.bridgeConfig.save_config(backup=False, resource="behavior_instance")
        elif resource in ["room", "zone"]:
            v1Api = {}
            if "metadata" in putDict:
                if "name" in putDict["metadata"]:
                    v1Api["name"] = putDict["metadata"]["name"]
                if "archetype" in putDict["metadata"]:
                    v1Api["icon_class"] = putDict["metadata"]["archetype"].replace("_", " ").capitalize()
            if "children" in putDict:
                light_ids = set(bridgeConfig["lights"].keys())
                new_light_map = {}
                new_device_children = []
                for child in putDict["children"]:
                    obj = getObject(child["rtype"], child["rid"])
                    if obj and any(obj is bridgeConfig["lights"][k] for k in light_ids):
                        new_light_map[child["rid"]] = obj
                    elif child["rtype"] == "device":
                        new_device_children.append(child["rid"])
                # Remove lights no longer in the new children list, deduplicate
                seen_ids = set()
                deduped = []
                for ref in object.lights:
                    if ref():
                        dev_info = ref().getDevice()
                        if dev_info and dev_info["id"] in new_light_map and dev_info["id"] not in seen_ids:
                            seen_ids.add(dev_info["id"])
                            deduped.append(ref)
                object.lights = deduped
                # Add only lights not already present
                existing_device_ids = set()
                for ref in object.lights:
                    if ref():
                        dev_info = ref().getDevice()
                        if dev_info:
                            existing_device_ids.add(dev_info["id"])
                for rid, obj in new_light_map.items():
                    if rid not in existing_device_ids:
                        object.add_light(obj)
                # Replace non-light device children (switches, sensors) wholesale
                object.device_children = new_device_children
            object.update_attr(v1Api)
            configManager.bridgeConfig.mark_dirty("groups")
        elif resource == 'geofence_client':
            attrs = {}
            if "name" in putDict:
                attrs['name'] = putDict['name']
            if 'is_at_home' in putDict:
                attrs['is_at_home'] = putDict['is_at_home']
            if hasattr(object, 'update_attr') and callable(getattr(object, 'update_attr')):
                object.update_attr(attrs)
        elif resource == "zigbee_device_discovery":
            if putDict["action"]["action_type"] == "search":
                bridgeConfig["config"]["zigbee_device_discovery_info"]["status"] = "active"
                configManager.bridgeConfig.mark_dirty("config")
                Thread(target=scanForLights).start()
        elif resource == "device":
            if "identify" in putDict and putDict["identify"]["action"] == "identify":
                _identifyMotionAwareLight(object)
            if "metadata" in putDict:
                if "name" in putDict["metadata"]:
                    if object:
                        object.name = putDict["metadata"]["name"]
                        configManager.bridgeConfig.mark_dirty("lights")
                    elif resourceid == v2BridgeDevice()["id"]:
                        bridgeConfig["config"]["name"] = putDict["metadata"]["name"]
                        configManager.bridgeConfig.mark_dirty("config")
                        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                        "data": [{
                                            "id": resourceid,
                                            "metadata": {
                                                "name": bridgeConfig["config"]["name"]
                                                },
                                            "type": "device"
                                        }],
                                        "id": str(uuid.uuid4()),
                                        "type": "update"
                                        }
                        StreamEvent(streamMessage)
        elif resource == "motion":
            if "enabled" in putDict:
                object.update_attr({"config": {"on": putDict["enabled"]}})
                configManager.bridgeConfig.mark_dirty("sensors")
        elif resource == "contact":
            if "enabled" in putDict:
                object.update_attr({
                    "config": {
                        "on": bool(putDict["enabled"])
                    }
                })
                configManager.bridgeConfig.mark_dirty("sensors")
                object.genContactStreamEvent()
        else:
            return {
                "errors": [{
                    "description": f"Resource type not supported: {resource}"
                }]
            }, 500

        response = {"data": [{
            "rid": resourceid,
            "rtype": resource
        }]}

        return response

    def delete(self, resource, resourceid):
        # logging.debug(request.headers)
        authorisation = authorizeV2(request.headers)
        _motion_diag(resource, resourceid, method="DELETE")
        if "user" not in authorisation:
            return "", 403
        if resource == "motion_area_configuration":
            if not deleteMotionAwareArea(resourceid):
                return {"errors": [{"description": "Resource not found"}]}, 404
            _motion_diag(resource, resourceid, method="DELETE", status=200)
            return {"data": [{"rid": resourceid, "rtype": resource}], "errors": []}
        object = getObject(resource, resourceid)

        if hasattr(object, 'getObjectPath'):
            # getObjectPath returns V1 resource names (e.g. "lights", "groups")
            # which map directly to CONFIG_FILES keys
            v1_resource = object.getObjectPath()["resource"]
            del bridgeConfig[v1_resource][object.getObjectPath()["id"]]
            configManager.bridgeConfig.mark_dirty(v1_resource)
        else:
            # V2 resource type (e.g. "room", "smart_scene") — map to V1 key
            del bridgeConfig[resource][resourceid]
            yaml_resource = configManager.configHandler.Config.V2_TO_V1_RESOURCE.get(resource)
            if yaml_resource:
                configManager.bridgeConfig.mark_dirty(yaml_resource)

        # Persist deletions immediately for resources whose in-memory state must
        # survive a bridge restart (scenes, behavior instances).
        if resource == "behavior_instance":
            configManager.bridgeConfig.save_config(backup=False, resource="behavior_instance")
        elif resource == "scene":
            configManager.bridgeConfig.save_config(backup=False, resource="scenes")

        response = {"data": [{
            "rid": resourceid,
            "rtype": resource
        }]}
        return response
