# Maintained holken runtime

`holken` is the production-source branch. `master` and `dev` mirror their
upstream branches. Do not deploy either mirror over this installation without
running the configuration migration and API compatibility checks.

The initial source snapshot is based on upstream
`354e7403ef07340241f42f2cb0dd66ba4e3d7f1f`. Its 90 Python files match the
verified runtime. `runtime-source-sha256.json` records that baseline;
`runtime-dependencies.json` records the installed dependency versions.
Intentional application updates must update the source manifest after review.

The first reviewed update backports upstream #1136 event-stream authentication
without changing the local resumable event transport. Existing application
keys remain valid; missing or invalid keys receive HTTP 403.

## Image and persistent data

Build with `.build/Dockerfile.holken` for `linux/arm64`. The immutable official
base digest preserves the verified Python dependencies and bundled Web UI.
The build copies application source only, with no package installation or
unversioned UI download. Runtime state, certificates, API users and bridge
identity are excluded from the build context and Git.

Persist `/opt/hue-emulator/config` outside the image. Preserve its entire
contents and the existing MAC/IP environment settings during replacement.
There must be no Python source bind mounts in the maintained deployment.
Do not start a second bridge with the same identity on the production network.

## Validation and publishing

Pushes to `holken` validate source, syntax and a native-format arm64 image.
They do not publish an image. The manual `Validate holken image` workflow can
publish a tested candidate as
`ghcr.io/yellownest/diyhue-holken:sha-<full-commit>` using `GITHUB_TOKEN`.
No `stable` tag is published automatically. The inherited unrestricted
Docker release workflow is disabled in this fork.

`Promote verified holken candidate` moves `stable` only when an operator supplies
an immutable candidate digest, confirms the live API/data checks, and that
candidate's exact source revision has successful validation CI. It promotes
the existing manifest without rebuilding or changing application code.

After candidate CI, test a copy of the real configuration with `--network none`,
no published ports and a separate writable config directory. Verify V1/V2
resource IDs, counts, names, scene actions, application-key authentication and
certificate continuity. Physical lighting and phone pairing still require
verification in the actual installation; an isolated test cannot prove them.

## Stable promotion and Watchtower

After deployment validation, record the candidate registry digest and keep the
previous digest plus a consistent config backup. Promote that verified digest
to `stable` explicitly. Immutable `sha-*` tags remain available for rollback.
Only enable Watchtower after verifying that its daemon can pull the registry
image. Private GHCR packages require separate registry credentials; a successful
Actions push does not prove anonymous or Watchtower pull access.

Watchtower remains in monitoring mode during initial deployment. It can replace
an image, but cannot merge source patches or validate physical functionality.
Keep a locally saved previous image: Watchtower's `--cleanup` can remove old
images after an update.

## Upstream integration

Track both upstream release branches separately. At the audit date, `dev`
contains the Device model, explicit bridge profiles and MotionAware foundation
that are absent from `master`/`latest`. These are different application models,
not interchangeable image tags.

Current upstream `dev` cannot load the existing custom smoke-sensor config
(`SmokeSensor-EF-3.0`). Keep the local generic sensor discovery, state-sync and
publishing behavior until each has an equivalent tested upstream implementation.
Integrate reviewed upstream changes on a separate upgrade branch, run the full
suite and isolated configuration/API comparison, then promote a new candidate.
Do not schedule unattended upstream merges or image publication.

## MotionAware limits

The retained local RF experiment interprets Zigbee2MQTT `linkquality` and is
not a validated implementation of Hue Bridge Pro radio motion sensing. A quiet
`motion_valid` resource or completed phone-app calibration does not prove
physical detection. The captured area is disabled. This experiment must not
be submitted upstream as verified MotionAware detection.

Current upstream provides an explicit provider interface and room occupancy
sensor aggregation, and intentionally excludes LQI-based RF detection.
