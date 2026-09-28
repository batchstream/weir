"""Offline execution of every branch of the actual calibration search."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from capacity_contract import PLAN

spec=importlib.util.spec_from_file_location("capacity_entry",Path(__file__).with_name("test-capacity.py"))
entry=importlib.util.module_from_spec(spec)
if __debug__:spec.loader.exec_module(entry)


@unittest.skipUnless(__debug__, "entry rejects optimized Python before imports")
class Flow(unittest.TestCase):
    def run_case(self, failure):
        c=entry.Calibration(None,PLAN)
        calls=[]
        def trial(name,rate,seconds,**options):
            calls.append(name)
            passed=not failure(name)
            report=dict(prefix=name,pass_=passed,reasons=[],overload_applied=True,recovery=dict(pass_=True,seconds=10))
            report["pass"]=report.pop("pass_");report["recovery"]["pass"]=report["recovery"].pop("pass_")
            if name=="overload":
                report["overload_applied"]=not failure("underload")
                report["recovery"]["pass"]=not failure("recovery")
            return report
        with patch.object(c,"trial",side_effect=trial):result=c.run()
        return result,calls

    def test_success(self):
        result,calls=self.run_case(lambda name:False)
        self.assertEqual(result["candidate_rps"],3200)
        self.assertEqual(len([n for n in calls if n.startswith("confirm")]),3)
        self.assertEqual(len([n for n in calls if n.startswith("direct")]),5)

    def test_no_candidate_and_confirmation_failure(self):
        result,calls=self.run_case(lambda name:name.startswith("search"))
        self.assertIsNone(result["candidate_rps"]);self.assertEqual(calls,["search-50"])
        result,calls=self.run_case(lambda name:name.startswith("confirm"))
        self.assertIsNone(result["candidate_rps"])
        self.assertEqual(len([n for n in calls if n.startswith("confirm")]),6)
        self.assertNotIn("overload",calls)

    def test_one_fallback_then_success(self):
        result,calls=self.run_case(lambda name:name.startswith("confirm-0"))
        self.assertEqual(result["candidate_rps"],1600)

    def test_insufficient_overload_or_bad_recovery_never_qualifies(self):
        for fail in ("underload","recovery"):
            result,calls=self.run_case(lambda name:name==fail)
            self.assertIsNone(result["candidate_rps"])
            self.assertFalse(any(n.startswith("direct") for n in calls))

    def test_fatal_evidence_never_downgrades(self):
        c=entry.Calibration(None,PLAN)
        with patch.object(c,"trial",side_effect=RuntimeError("invalid observations")) as run:
            with self.assertRaises(RuntimeError):c.run()
            self.assertEqual(run.call_count,1)


if __name__ == "__main__":unittest.main()
