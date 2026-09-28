"""Complete diagnostic lifecycle over real admitted shapes and external fake CLI."""
import argparse
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch

import eks_loopback as loop
import eks_pacing as common
import eks_socket_diagnostic as diag
import eks_loopback_admission_test as admission
from eks_loopback_test import plan


class DiagnosticLifecycle(unittest.TestCase):
    def setUp(self):
        # Reuse the exact recorded Job / admitted Pod / CLI / cleanup wiring.
        self.fixture = admission.AdmissionReplay()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        f = self.fixture
        f.cfg['diagnostic_only'] = True
        f.cfg['bootstrap_log'] = diag.RECEIPT.rstrip()
        for s in f.cfg['actual_pod']['status']['containerStatuses']:
            s.update(state=dict(waiting=dict(reason='PodInitializing')),imageID='',containerID='',ready=False,started=False)
        for s in f.cfg['actual_pod']['status']['initContainerStatuses']:
            if s['name']=='bootstrap':
                s['state'] = dict(terminated=dict(reason='Error',exitCode=42,finishedAt='2026-09-28T05:57:24Z'))
        f.write_config()
        proof=f.root/'proof.json'
        proof.write_text('{"files":{}}')
        f.run.registry_evidence=dict(path=str(proof),sha256=common.digest(proof),proof=dict(files={}))

    def execute(self):
        f = self.fixture
        f.write_config()
        diag.prepare(f.run,plan()['owner'])
        opts = argparse.Namespace(plan_sha256=common.digest(f.evidence/'plan.json'),node_uid='synthetic-node')
        code = diag.execute(f.run,opts)
        result = json.loads((f.evidence/'result.json').read_text())
        self.assertFalse(result['functional_pass'])
        self.assertEqual(result['document_mutations'],0)
        self.assertEqual(result['management_mutations'],0)
        self.assertIsNone(result['candidate'])
        self.assertFalse(any('exec' in c or 'trial' in c or 'setup' in c for c in f.calls()))
        config=json.loads((f.evidence/'create-ConfigMap-configuration.json').read_text())
        self.assertEqual(config['data']['bootstrap.sh'],Path(__file__).with_name('eks_loopback_diagnostic.sh').read_text())
        self.assertNotIn('PUT',config['data']['bootstrap.sh'])
        self.assertFalse((f.evidence/'bootstrap-management.json').exists())
        return code,result

    def test_planned_stop_runs_both_dry_runs_collects_and_uid_cleans(self):
        code,result=self.execute()
        self.assertEqual(code,0)
        self.assertTrue(result['diagnostic_complete'])
        self.assertTrue(result['evidence_collected'])
        self.assertTrue(result['cleanup']['confirmed'])
        self.assertEqual(result['bootstrap_exit'],42)
        creates=self.fixture.creates()
        self.assertIn(('Job',True),creates)
        self.assertIn(('Pod',True),creates)
        self.assertEqual(sum(k=='Job' and not d for k,d in creates),1)
        self.assertEqual(len(result['cleanup']['resources']),6)
        self.assertEqual(json.loads((self.fixture.root/'state.json').read_text()),{})

    def test_guard23_preserves_es143_and_cleans_without_continuation(self):
        f=self.fixture
        for s in f.cfg['actual_pod']['status']['initContainerStatuses']:
            bootstrap=s['name']=='bootstrap'
            s['state']=dict(terminated=dict(reason='Error',exitCode=23 if bootstrap else 143,
                finishedAt='2026-09-28T05:57:24Z' if bootstrap else '2026-09-28T05:57:35Z'))
        f.cfg['bootstrap_log']='socket-reject table=/proc/net/tcp6 exit=23 reason=tcp-local-address'
        code,result=self.execute()
        self.assertEqual(code,1)
        self.assertFalse(result['diagnostic_complete'])
        self.assertTrue(result['evidence_collected'])
        self.assertEqual(result['bootstrap_exit'],23)
        self.assertTrue(result['cleanup']['confirmed'])
        self.assertIn('"exitCode": 143',result['observation_stop'])
        report=json.loads((f.evidence/'final-container-outcomes.json').read_text())
        self.assertEqual(report['earliest_failed_finish'],'bootstrap')

    def test_timeout_retains_status_and_cleans(self):
        f=self.fixture
        f.cfg['actual_pod']['status']['initContainerStatuses'][1]['state']=dict(running=dict(startedAt='synthetic'))
        # Drive the existing absolute deadline without a 150-second test sleep.
        real=time.monotonic
        def clock():
            now=real()
            if (f.evidence/'owned.json').exists() and len(json.loads((f.evidence/'owned.json').read_text()))>=5:
                return now+151
            return now
        with patch.object(time,'monotonic',side_effect=clock):
            code,result=self.execute()
        self.assertEqual(code,1)
        self.assertIn('150 second',result['observation_stop'])
        self.assertTrue((f.evidence/'final-pod.json').exists())
        self.assertTrue(result['cleanup']['confirmed'])
        self.assertFalse(result['diagnostic_complete'])

    def test_uid_drift_does_not_read_logs_or_delete_replacement(self):
        f=self.fixture
        f.cfg['diagnostic_uid_drift']=True
        code,result=self.execute()
        self.assertEqual(code,1)
        self.assertIn('UID drift',result['collection_error'])
        self.assertFalse(result['cleanup']['confirmed'])
        self.assertFalse(any('logs' in call for call in f.calls()))
        state=json.loads((f.root/'state.json').read_text())
        self.assertIn('Pod/synthetic-pod',state)
        self.assertIn('Namespace/'+plan()['namespace'],state)

    def test_no_receipt_is_not_diagnostic_success(self):
        self.fixture.cfg['bootstrap_log']='truncated'
        code,result=self.execute()
        self.assertEqual(code,1)
        self.assertFalse(result['diagnostic_complete'])
        self.assertTrue(result['cleanup']['confirmed'])

    def test_main_runtime_is_rejected(self):
        state=self.fixture.cfg['actual_pod']['status']['containerStatuses'][0]
        state.update(containerID='containerd://unexpected',state=dict(running=dict(startedAt='synthetic')))
        code,result=self.execute()
        self.assertEqual(code,1)
        self.assertIn('main started/history',result['collection_error'])
        self.assertFalse(result['diagnostic_complete'])
        self.assertTrue(result['cleanup']['confirmed'])

    def test_real_oom_collects_logs_but_never_completes(self):
        state=self.fixture.cfg['actual_pod']['status']['initContainerStatuses'][0]
        state['state']=dict(terminated=dict(reason='OOMKilled',exitCode=137))
        code,result=self.execute()
        self.assertEqual(code,1)
        self.assertIn('OOMKilled',result['collection_error'])
        self.assertTrue((self.fixture.evidence/'final-elasticsearch.log').exists())
        self.assertFalse(result['diagnostic_complete'])
        self.assertTrue(result['cleanup']['confirmed'])

    def test_real_admission_negatives_still_prevent_persistent_job(self):
        f=self.fixture
        f.cfg['pod_response']['spec']['initContainers'][0]['resources']['limits']['memory']='3221225473'
        code,result=self.execute()
        self.assertEqual(code,1)
        self.assertTrue(result['cleanup']['confirmed'])
        self.assertFalse(any(k=='Job' and not d for k,d in f.creates()))
        self.assertFalse(result['evidence_collected'])


if __name__=='__main__':unittest.main()
