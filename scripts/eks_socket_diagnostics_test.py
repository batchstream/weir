"""Execute the shell guard against frozen/synthetic bytes; no live sockets."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import eks_loopback as loop
from eks_loopback_test import live_pod, options, tcp_table

HEADER = tcp_table().splitlines()[0]+"\n"
OLD = Path(__file__).with_name("fixtures")/"eks-loopback-check-before-m26r3.sh"
CHECK = Path(__file__).with_name("eks_loopback_check.sh")


def row(local="0100007F:23F0", remote="00000000:0000", state="0A"):
    return f"   0: {local} {remote} {state} 00000000:00000000 00:00000000 00000000 1000 0 12345 1 0000000000000000 100 0 0 10 0\n"


class ShellDiagnostics(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        pairs = ("network.host=127.0.0.1", "http.host=127.0.0.1", "transport.host=127.0.0.1", "discovery.type=single-node",
                 "action.auto_create_index=false", "xpack.security.enabled=false", "xpack.ml.enabled=false",
                 "ingest.geoip.downloader.enabled=false", "node.store.allow_mmap=false")
        settings = dict(nodes=dict(synthetic=dict(settings=dict(pair.split("=") for pair in pairs))))
        (self.root/"settings.json").write_text(json.dumps(settings))
        curl = '''import json,pathlib,sys
root=pathlib.Path(__file__).parent
with (root/"calls.jsonl").open("a") as f:f.write(json.dumps(sys.argv[1:])+"\\n")
if not sys.argv[-1].startswith("http://127.0.0.1:9200/") or any(v in sys.argv for v in ("PUT","POST","DELETE")):raise SystemExit(90)
if sys.argv[-1].endswith("9200/"):print('{"version":{"number":"8.19.22"}}')
else:print((root/"settings.json").read_text())
'''
        for name, script in (("curl", curl), ("nc", "raise SystemExit(1)\n"), ("timeout", "raise SystemExit(1)\n")):
            exe = self.root/name
            exe.write_text("#!"+sys.executable+"\n"+script)
            exe.chmod(0o700)
        self.tables = dict(tcp=HEADER+row()+row("0100007F:2454"), tcp6=HEADER, udp=HEADER, udp6=HEADER)

    def execute(self, source=None, prelude="", mode="bootstrap"):
        for name, data in self.tables.items():
            target = self.root/name
            if target.exists():
                target.unlink()
            if data is not None:
                target.write_text(data)
        script = (source or CHECK.read_text()).replace("/proc/net/",str(self.root)+"/")
        target = self.root/"check.sh"
        target.write_text(prelude+script)
        env = dict(PATH=str(self.root)+os.pathsep+os.environ["PATH"], POD_IP="10.0.0.2")
        result = subprocess.run([shutil.which("bash"),"--noprofile","--norc",str(target),mode],env=env,capture_output=True,text=True,timeout=6)
        return result

    def test_old_rules_unchanged_on_identical_complete_snapshots(self):
        baseline = copy.deepcopy(self.tables)
        cases = [("tcp", row("00000000:23F0"),23), ("tcp6",row("0"*32+":1234",state="07"),23),
                 ("tcp6",row("0000000000000000FFFF00000100007F:2454"),0),
                 ("tcp6",row("00000000000000000000000001000000:1234"),23),
                 ("tcp",row(remote="0200000A:1234",state="01"),24),
                 ("tcp",row("0100007F:1234",state="0A"),29),
                 ("tcp",row("0100007F:1D17"),28), ("udp",row(),32), ("udp6",row(),32)]
        for state in ("01","02","03","04","05","06","07","08","09","0B","0C","FF"):
            # Original rules do not whitelist non-LISTEN states. Retain that
            # decision, without describing an unknown state as proven safe.
            cases.append(("tcp",row("0100007F:C001","00000000:0000",state),0))
        for table, extra, code in cases:
            with self.subTest(table=table, row=extra):
                self.tables = copy.deepcopy(baseline)
                self.tables[table] += extra
                old = self.execute(OLD.read_text())
                new = self.execute()
                self.assertEqual(old.returncode,code,old.stderr)
                self.assertEqual(new.returncode,old.returncode,new.stdout+new.stderr)
                self.assertIn(extra.rstrip(),new.stdout)
                if code:
                    self.assertIn("exit="+str(code),new.stdout)
                    self.assertNotIn("loopback-check-complete",new.stdout)

    def test_rejected_row_metadata_and_all_raw_tables_precede_rejection(self):
        bad = row("0"*32+":ABCD",state="07")
        self.tables["tcp6"] += bad
        result = self.execute()
        self.assertEqual(result.returncode,23,result.stderr)
        self.assertIn("state=07 uid=1000 inode=12345 process=unknown raw="+bad.rstrip(),result.stdout)
        self.assertLess(result.stdout.index("/udp6 bytes="),result.stdout.index("socket-reject"))
        self.assertIn("atomic=false",result.stdout)
        self.assertIn(HEADER,result.stdout)

    def test_missing_short_empty_truncated_and_bounded_tables(self):
        baseline = copy.deepcopy(self.tables)
        cases = [(None,33),("",33),("\n",33),(HEADER+"0: 0100007F:0001\n",33),
                 (HEADER+row().rstrip(),33),(HEADER+"x"*65537,35),(HEADER+row()*256,37)]
        for raw, code in cases:
            with self.subTest(code=code, length=len(raw or "")):
                self.tables = copy.deepcopy(baseline)
                self.tables["tcp6"] = raw
                result = self.execute()
                self.assertEqual(result.returncode,code,result.stdout+result.stderr)
                self.assertIn("socket-table-begin",result.stdout)
                self.assertLess(len(result.stdout),70000)
        self.tables = dict(tcp=HEADER,tcp6=HEADER,udp=HEADER,udp6=HEADER)
        self.assertEqual(self.execute().returncode,30)

    def test_decision_uses_captured_bytes_even_if_source_changes_after_logging(self):
        self.tables["tcp6"] += row("0"*32+":ABCD",state="07")
        prelude = 'printf() { builtin printf "$@"; if [[ $1 == *socket-table-end* && $2 == */tcp6 ]]; then : > "'+str(self.root/'tcp6')+'"; fi; }\n'
        result = self.execute(prelude=prelude)
        self.assertEqual(result.returncode,23,result.stdout+result.stderr)

    def test_log_failure_does_not_replace_guard_exit_or_allow_success(self):
        prelude = 'printf() { if [[ $1 == socket-* || $1 == *socket-table-end* ]]; then return 1; fi; builtin printf "$@"; }\n'
        self.assertEqual(self.execute(prelude=prelude).returncode,36)
        self.tables["tcp6"] += row("0"*32+":ABCD",state="07")
        self.assertEqual(self.execute(prelude=prelude).returncode,23)

    def test_read_timeout_retains_previous_tables_and_stops(self):
        # Test-only read override. Production paths and read limits stay fixed.
        prelude = 'read() { if [[ $* == *socket_raw* && $table == */tcp6 ]]; then socket_raw="partial"; return 142; fi; builtin read "$@"; }\n'
        result = self.execute(prelude=prelude)
        self.assertEqual(result.returncode,34,result.stderr)
        self.assertIn(self.tables["tcp"],result.stdout)
        self.assertIn("partial",result.stdout)

    def test_terminal_readonly_script_exits_42_or_preserves_guard_failure(self):
        diagnostic = Path(__file__).with_name("eks_loopback_diagnostic.sh").read_text()
        # Namespace identifier is available only in the real owned Linux container.
        diagnostic = diagnostic.replace("readlink /proc/self/ns/net", "printf 'net:[synthetic]\\n'")
        check = CHECK.read_text().replace("/proc/net/",str(self.root)+"/")
        (self.root/"loopback-check.sh").write_text(check)
        diagnostic = diagnostic.replace("/qualification/",str(self.root)+"/")
        result = self.execute(source=diagnostic)
        self.assertEqual(result.returncode,42,result.stdout+result.stderr)
        self.assertIn('"diagnostic":"complete"',result.stdout)
        self.tables["tcp6"] += row("0"*32+":ABCD",state="07")
        result = self.execute(source=diagnostic)
        self.assertEqual(result.returncode,23,result.stdout+result.stderr)
        self.assertNotIn('"diagnostic":"complete"',result.stdout)
        calls = [json.loads(line) for line in (self.root/"calls.jsonl").read_text().splitlines()]
        self.assertEqual(len(calls),4)
        self.assertFalse(any(method in call for call in calls for method in ("PUT","POST","DELETE")))

    def test_python_reject_preserves_same_raw_fields_and_truncation(self):
        self.tables["tcp6"] += row("0"*32+":ABCD",state="07")
        files = {"net/"+key:value for key,value in self.tables.items()}
        with self.assertRaisesRegex(ValueError,"tcp-local-address exit=23") as failure:
            loop.network_check(files)
        for token in ("ABCD",'"state": "07"','"uid": "1000"','"inode": "12345"',"local_address"):
            self.assertIn(token,str(failure.exception))
        for raw in (None,"",HEADER+"0: x\n",HEADER+row().rstrip(),"x"*65537):
            files["net/tcp6"] = raw
            with self.assertRaises(ValueError):loop.network_check(files)


class ContainerDiagnostics(unittest.TestCase):
    def failed_pod(self):
        pod = live_pod()
        for state in pod["status"]["initContainerStatuses"]:
            bootstrap = state["name"] == "bootstrap"
            state["state"] = dict(terminated=dict(reason="Error",exitCode=23 if bootstrap else 143,
                finishedAt="2026-09-28T05:57:24Z" if bootstrap else "2026-09-28T05:57:35Z"))
        return pod

    def test_all_exits_retained_in_both_orders_without_oom_guess(self):
        pod = self.failed_pod()
        for reverse in (False,True):
            if reverse:pod["status"]["initContainerStatuses"].reverse()
            report = loop.container_outcomes(pod["status"])
            self.assertEqual(report["earliest_failed_finish"],"bootstrap")
            with self.assertRaises(ValueError) as failure:loop.pod_check(pod,options())
            text = str(failure.exception)
            for token in ('"exitCode": 23','"exitCode": 143','"restartCount": 0','"lastState": {}',"05:57:24Z","05:57:35Z"):
                self.assertIn(token,text)
            self.assertNotIn("OOM",text)

    def test_success_unknown_order_real_oom_and_restart(self):
        self.assertEqual(loop.container_outcomes(live_pod()["status"])["failures"],[])
        for change in (lambda s:s[0]["state"]["terminated"].pop("finishedAt"),
                       lambda s:s[0]["state"]["terminated"].update(finishedAt="2026-09-28T05:57:24Z"),
                       lambda s:s[0].update(restartCount=1,lastState=dict(terminated=dict(reason="OOMKilled",exitCode=137)))):
            pod = self.failed_pod()
            change(pod["status"]["initContainerStatuses"])
            report = loop.container_outcomes(pod["status"])
            self.assertIsNone(report["earliest_failed_finish"])
            with self.assertRaises(ValueError):loop.pod_check(pod,options())
        pod = live_pod()
        pod["status"]["initContainerStatuses"][0]["state"] = dict(terminated=dict(exitCode=137,reason="OOMKilled"))
        with self.assertRaisesRegex(ValueError,"OOMKilled"):loop.pod_check(pod,options())


if __name__ == "__main__":unittest.main()
