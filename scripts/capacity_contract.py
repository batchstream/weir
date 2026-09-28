"""The maintained entry uses only this versioned, fixed M22R profile."""
import json
from pathlib import Path

PLAN_PATH = Path(__file__).with_name("capacity-plan-m22r.json")
PLAN = json.loads(PLAN_PATH.read_text())


class Budget:
    """Reserve before invocation; missing final receipts stay unknown, never zero."""

    def __init__(self):
        self.entries = []

    def reserve(self, name, *, planned=0, seeds=0, seconds=0):
        if any(e["name"] == name for e in self.entries):
            raise RuntimeError("duplicate budget reservation")
        if min(planned, seeds, seconds) < 0 or planned % 10:
            raise RuntimeError("invalid reservation")
        entry = dict(name=name, planned=planned+seeds, load_planned=planned, seed_planned=seeds,
                     reserved=seeds+planned//10, load_seconds=seconds,
                     actually_started=None, started_lower_bound=0)
        trial = self.entries + [entry]
        limits = PLAN["budgets"]
        if (sum(e["planned"] for e in trial) > limits["planned"] or
                sum(e["reserved"] for e in trial) > limits["mutations"] or
                sum(e["load_seconds"] for e in trial) > limits["load_seconds"] or
                entry["reserved"] > limits["trial_mutations"]):
            raise RuntimeError("cumulative/trial mutation or load budget")
        self.entries.append(entry)
        return entry

    def reconcile(self, entry, records, complete):
        setup = [r for r in records if r.get("type") == "setup_progress"]
        previous = 0
        for r in setup:
            started = r["started"]
            if not previous <= started <= entry["seed_planned"]:
                raise RuntimeError("setup count regression/overrun")
            previous = started
        trials = [r["trial"] for r in records if r.get("type") == "trial"]
        started = previous + sum(t[w]["put"]["started"] for t in trials for w in ("warm", "measure"))
        if started > entry["reserved"]:
            raise RuntimeError("mutation reservation exceeded")
        entry["started_lower_bound"] = started
        # Normal error returns also produce a final receipt; killed commands do not.
        final = [r for r in records if r.get("type") == "mutation_receipt"]
        if final:
            if len(final) != 1 or final[0]["started"] != started:
                raise RuntimeError("mutation receipt mismatch")
            entry["actually_started"] = started
        elif complete:
            raise RuntimeError("missing mutation receipt")

    def snapshot(self):
        result = dict(planned=sum(e["planned"] for e in self.entries),
                      seed_planned=sum(e["seed_planned"] for e in self.entries),
                      reserved=sum(e["reserved"] for e in self.entries),
                      load_seconds=sum(e["load_seconds"] for e in self.entries),
                      entries=self.entries)
        return result
