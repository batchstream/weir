"""Selective, named request accounting for the two bounded EKS profiles.

Pinned Kubernetes helpers v1.35.3/v1.36.0: per-resource max of spec,
allocated and actuated, sequential init peaks, Pod overrides then overhead.
We retain spec even for Infeasible (a documented conservative upper bound).
This is request accounting, never usage, a reservation, or exact scheduler state.
"""
import json
from decimal import Decimal, ROUND_CEILING

from eks_pacing_report import quantity, require

TRACKED = ("cpu", "memory", "ephemeral-storage")
MAX = Decimal(2**63-1)


def projection(json_template):
    # The API returns only these resource/identity fields. No business names,
    # namespace, env, commands, volumes, annotations, full spec, or logs.
    container = '''{{define "resourceContainer"}}{"name":{{template "json" .name}},"restartPolicy":{{template "json" .restartPolicy}},"resources":{{template "json" .resources}},"allocatedResources":{{template "json" .allocatedResources}},"claims":{{if .resources.claims}}true{{else}}false{{end}},"resizePolicy":{{template "json" .resizePolicy}}}{{end}}'''
    fields = dict(uid=".metadata.uid", nodeName=".spec.nodeName", phase=".status.phase",
                  deleting=".metadata.deletionTimestamp", resources=".spec.resources",
                  statusResources=".status.resources", allocatedResources=".status.allocatedResources",
                  overhead=".spec.overhead", resize=".status.resize")
    body = ','.join('"'+k+'":{{template "json" '+v+'}}' for k, v in fields.items())
    for name, path in (("containers", ".spec.containers"), ("initContainers", ".spec.initContainers"),
                       ("containerStatuses", ".status.containerStatuses"), ("initContainerStatuses", ".status.initContainerStatuses")):
        body += ',"'+name+'": [{{range $i,$c := '+path+'}}{{if $i}},{{end}}{{template "resourceContainer" $c}}{{end}}]'
    body += ''',"resizeConditions":[{{$first := true}}{{range .status.conditions}}{{if or (eq .type "PodResizePending") (eq .type "PodResizeInProgress")}}{{if not $first}},{{end}}{{$first = false}}{"type":{{template "json" .type}},"status":{{template "json" .status}},"reason":{{template "json" .reason}}}{{end}}{{end}}],"unsupported":{{if or .spec.resourceClaims .status.resourceClaimStatuses .status.nodeAllocatableResourceClaimStatuses .spec.ephemeralContainers .status.ephemeralContainerStatuses}}true{{else}}false{{end}}'''
    return json_template+container+'[{{range $i,$p := .items}}{{if $i}},{{end}}{{with $p}}{'+body+'}{{end}}{{end}}]'


def resource_map(raw):
    require(raw is None or isinstance(raw, dict), "resource map type")
    values = {}
    for key, value in (raw or {}).items():
        require(key in TRACKED, "unsupported resource: "+key)
        require(isinstance(value, str) and len(value) <= 64, "resource quantity type/length")
        number = quantity(value)
        scale = 1000 if key == "cpu" else 1
        require(number.is_finite() and 0 <= number*scale <= MAX, "resource overflow")
        values[key] = (number*scale).to_integral_value(rounding=ROUND_CEILING)/scale
    return values


def requests(raw):
    require(raw is None or isinstance(raw, dict), "resource requirements type")
    raw = raw or {}
    require(set(raw) <= {"requests", "limits", "claims"} and not raw.get("claims"), "unsupported resource requirements")
    req = resource_map(raw.get("requests"))
    limits = resource_map(raw.get("limits"))
    # Admission normally copies limits to missing requests. An incomplete
    # admitted projection cannot establish whether this has happened.
    require(set(limits) <= set(req), "limit without admitted request")
    return req


def maximum(*maps):
    result = {k: max(m.get(k, Decimal(0)) for m in maps) for k in TRACKED}
    return result


def plus(*maps):
    result = {k: sum((m.get(k, Decimal(0)) for m in maps), Decimal(0)) for k in TRACKED}
    require(all(v*(1000 if k == "cpu" else 1) <= MAX for k, v in result.items()), "aggregate overflow")
    return result


def named(items):
    require(isinstance(items, list), "container/status list missing")
    result = {}
    for item in items:
        require(isinstance(item, dict) and isinstance(item.get("name"), str) and item["name"], "missing container name")
        require(item["name"] not in result, "duplicate container/status name")
        result[item["name"]] = item
    return result


def pod_requests(pod):
    require(not pod["unsupported"], "DRA/ephemeral container semantics unsupported")
    regular, initial = named(pod["containers"]), named(pod["initContainers"])
    require(regular and not set(regular) & set(initial), "missing/duplicate spec names")
    status, init_status = named(pod["containerStatuses"]), named(pod["initContainerStatuses"])
    require(set(status) <= set(regular) and set(init_status) <= set(initial), "status/spec name mismatch")
    resize = pod["resize"]
    require(resize in (None, "", "Proposed", "InProgress", "Deferred", "Infeasible"), "unknown resize state")
    conditions = pod["resizeConditions"]
    require(isinstance(conditions, list), "resize conditions missing")
    require(len({c["type"] for c in conditions}) == len(conditions), "duplicate resize condition")
    require(all(c["type"] in ("PodResizePending", "PodResizeInProgress") and c["status"] in ("True", "False", "Unknown") for c in conditions), "unknown resize condition")
    resizing = bool(resize) or any(c["status"] != "False" for c in conditions)
    evidence = []
    effective = {}
    for group, states in ((regular, status), (initial, init_status)):
        for name, container in group.items():
            require(not container.get("claims"), "container DRA unsupported")
            require(container.get("restartPolicy") in (None, "Always") and
                    (group is initial or not container.get("restartPolicy")), "unknown container restart semantics")
            policy = container.get("resizePolicy") or []
            require(isinstance(policy, list) and all(p.get("resourceName") in ("cpu", "memory") and p.get("restartPolicy") in ("NotRequired", "RestartContainer") for p in policy), "unknown resize policy")
            observed = states.get(name, {})
            if resizing and (group is regular or container.get("restartPolicy") == "Always"):
                require(observed.get("resources") is not None and observed.get("allocatedResources") is not None, "resize resource status missing")
            spec = requests(container.get("resources"))
            actuated = requests(observed.get("resources"))
            allocated = resource_map(observed.get("allocatedResources"))
            effective[name] = maximum(spec, actuated, allocated)
            evidence.append(dict(name=name, spec=spec, allocated=allocated, actuated=actuated,
                                 effective=effective[name], status_present=name in states))
    steady = plus(*(effective[n] for n in regular))
    sidecars, peak = {}, {}
    phases = []
    for name, container in initial.items():
        phase = plus(sidecars, effective[name])
        phases.append(dict(init=name, requests=phase))
        peak = maximum(peak, phase)
        if container.get("restartPolicy") == "Always":
            sidecars = phase
    steady = plus(steady, sidecars)
    total = maximum(steady, peak)
    pod_spec = requests(pod["resources"])
    pod_actuated = requests(pod["statusResources"])
    pod_allocated = resource_map(pod["allocatedResources"])
    require(set(pod_spec) <= {"cpu", "memory"}, "unsupported Pod-level resource")
    require(set(pod_actuated) | set(pod_allocated) <= set(pod_spec), "Pod resource status without spec")
    if resizing and pod_spec:
        require(pod["statusResources"] is not None and pod["allocatedResources"] is not None, "Pod resize status missing")
    pod_effective = maximum(pod_spec, pod_actuated, pod_allocated)
    for key in pod_spec:
        total[key] = pod_effective[key]
    overhead = resource_map(pod["overhead"])
    total = plus(total, overhead)
    detail = dict(containers=evidence, init_phases=phases, steady=steady,
                  pod_spec=pod_spec, pod_allocated=pod_allocated, pod_actuated=pod_actuated,
                  overhead=overhead, effective=total, resize=resize, resize_conditions=conditions,
                  model="conservative max including spec even when Infeasible; completed init peak retained")
    return total, detail


def allocated(raw):
    pods = json.loads(raw)
    require(isinstance(pods, list), "structured resource projection required")
    used, details, seen = {}, [], set()
    for pod in pods:
        require(isinstance(pod, dict) and pod.get("uid") and "nodeName" in pod, "Pod identity missing")
        require(pod["uid"] not in seen, "duplicate Pod UID")
        seen.add(pod["uid"])
        node = pod["nodeName"]
        detail = dict(uid=pod["uid"], nodeName=node, phase=pod.get("phase"), deleting=pod.get("deleting"))
        details.append(detail)
        if not node:
            detail["excluded"] = "unbound; not a reservation"
            continue
        current = used.setdefault(node, dict(cpu=Decimal(0), memory=Decimal(0), pods=Decimal(0),
                                            **{"ephemeral-storage": Decimal(0)}, errors=[]))
        if pod.get("phase") in ("Succeeded", "Failed") and not pod.get("deleting"):
            detail["excluded"] = "terminal non-deleting Pod"
            continue
        current["pods"] += 1
        try:
            require(pod.get("phase") in ("Pending", "Running", "Succeeded", "Failed"), "unknown/missing Pod phase")
            total, calculation = pod_requests(pod)
            detail.update(calculation)
            current.update(plus(current, total))
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            detail["error"] = str(exc)
            current["errors"].append(dict(uid=pod["uid"], reason=str(exc)))
    return used, details


def serializable(value):
    return json.loads(json.dumps(value, default=str))
