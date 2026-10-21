import uuid
import logManager
import configManager
from HueObjects import genV2Uuid, StreamEvent
from datetime import datetime, timezone

logging = logManager.logger.get_logger(__name__)


def _canonical_group_reference(reference):
    """Map legacy diyHue group IDs to the CLIP room/zone resource ID.

    A behavior's ``where`` is a CLIP resource reference.  Older diyHue paths
    persisted ``Group.id_v2`` (the grouped_light ID) while retaining rtype
    ``room`` or ``zone``.  Hue consequently regards the target as externally
    removed.  Accept the legacy form on input, but expose and persist the
    canonical resource ID.
    """
    if not isinstance(reference, dict):
        return reference

    rtype = reference.get("rtype")
    rid = reference.get("rid")
    if rtype not in ("room", "zone") or not isinstance(rid, str):
        return reference

    for group in configManager.bridgeConfig.yaml_config.get("groups", {}).values():
        if rtype == "room" and getattr(group, "type", None) == "Room":
            canonical = group.getV2Room()["id"]
        elif rtype == "zone" and getattr(group, "type", None) == "Zone":
            canonical = group.getV2Zone()["id"]
        else:
            continue

        if rid in (group.id_v2, canonical):
            return {"rid": canonical, "rtype": rtype}

    return reference


def _canonicalize_group_references(value):
    """Recursively normalise CLIP ``where[].group`` references."""
    if isinstance(value, list):
        return [_canonicalize_group_references(item) for item in value]
    if not isinstance(value, dict):
        return value

    normalized = {
        key: _canonicalize_group_references(item)
        for key, item in value.items()
    }
    if isinstance(normalized.get("group"), dict):
        normalized["group"] = _canonical_group_reference(normalized["group"])
    return normalized

class BehaviorInstance():
    def __init__(self, data):
        self.id_v2 = data["id"] if "id" in data else genV2Uuid()
        self.id_v1 = self.id_v2  # used for config save
        self.name = data["metadata"]["name"] if "name" in data["metadata"] else None
        self.configuration = _canonicalize_group_references(data["configuration"])
        self.enabled = data["enabled"] if "enabled" in data else False
        self.active = data["active"] if "active" in data else False
        self.script_id = data["script_id"] if "script_id" in data else ""

        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getV2Api()],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        StreamEvent(streamMessage)

    def __del__(self):
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.id_v2, "type": "behavior_instance"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        StreamEvent(streamMessage)
        logging.info(self.name + " behaviour instance was destroyed.")

    def getV2Api(self):
        self.configuration = _canonicalize_group_references(self.configuration)
        result = {"configuration": self.configuration,
                  "dependees": [],
                  "enabled": self.enabled,
                  "id": self.id_v2,
                  "last_error": "",
                  "metadata": {
                      "name": "noname"
                  },
                  "script_id": self.script_id,
                  "status": "running" if self.enabled else "disabled",
                  "type": "behavior_instance"
                  }

        if self.name != None:
            result["metadata"]["name"] = self.name

        where_lists = []

        def collect_where(value):
            if isinstance(value, dict):
                where = value.get("where")
                if isinstance(where, list):
                    where_lists.append(where)

                for child in value.values():
                    collect_where(child)

            elif isinstance(value, list):
                for child in value:
                    collect_where(child)

        collect_where(self.configuration)

        seen_rids = set()
        for where_list in where_lists:
            for resource in where_list:
                key = list(resource.keys())[0]
                rid = resource[key]["rid"]
                if rid not in seen_rids:
                    seen_rids.add(rid)
                    result["dependees"].append({"level": "critical",
                                                "target": {
                                                    "rid": rid,
                                                    "rtype": resource[key]["rtype"]
                                                },
                                                "type": "ResourceDependee"
                                                })

        return result

    def update_attr(self, newdata):
        for key, value in newdata.items():
            if key == "metadata" and "name" in value:
                self.name = value["name"]
                continue
            # V2 PUTs commonly echo immutable resource identity/type fields.
            # They are not mutable Python attributes (the internal id is
            # `id_v2`), so ignore them instead of turning a valid edit into a
            # 500 response.  This is required by Hue's behavior time-slot
            # editor, which sends the complete behavior resource.
            if key in {"id", "type", "status", "last_error", "dependees"}:
                continue
            if not hasattr(self, key):
                continue
            updateAttribute = getattr(self, key)
            if isinstance(updateAttribute, dict):
                updateAttribute.update(value)
                setattr(self, key, updateAttribute)
            else:
                setattr(self, key, value)
        self.configuration = _canonicalize_group_references(self.configuration)
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getV2Api()],
                         "id": str(uuid.uuid4()),
                         "type": "update"
                         }
        StreamEvent(streamMessage)

    def save(self):
        result = {"id": self.id_v2, "metadata": {"name": self.name}, "configuration": self.configuration, "enabled": self.enabled, "active": self.active,
                  "script_id": self.script_id}
        if self.name != None:
            result["metadata"] = {"name": self.name}

        return result
