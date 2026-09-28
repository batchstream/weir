"""Completion of qualification observe streams only; no mutation/JSON-mode ACKs."""
import json
import os
import time

from capacity_report import integer, timestamp
from resource_report import observer_identity, process, require


def observation_samples(observer, options):
    entries = observer.poll()
    role, count = options['role'], options['samples']
    require(len(entries) <= count+2 and all(isinstance(e, dict) for e in entries), 'observer record count/type')
    if not entries:
        require(not all(observer.eof), 'missing observer identity')
        return [], False
    require(entries[0].get('type') == 'identity' and entries[0].get('role') == role, 'observer identity/role')
    complete = entries[-1].get('type') == 'observer_end'
    samples = entries[1:-1] if complete else entries[1:]
    require(len(samples) <= count, 'observer sample count')
    if samples:
        observer_identity(entries[0], samples[0], options)
    previous = None
    for sequence, sample in enumerate(samples):
        require('type' not in sample and sample.get('role') == role and
                integer(sample['sequence']) == sequence and not sample.get('errors'), 'observer sample role/sequence/errors')
        target, helper = process(sample['process']), process(sample['observer'])
        require(all(len(sample[who][field].encode()) <= 262144 for who in ('process','observer') for field in ('stat','status')),
                'observer raw process field bound')
        require(target == entries[0]['target'] and helper == entries[0]['observer'] and
                target['uid'] == helper['uid'] and target['namespaces'] == helper['namespaces'] and
                target['cgroup'] == helper['cgroup'] and target['pid'] != helper['pid'], 'observer sample identity drift')
        files = sample['files']
        require(isinstance(files, dict) and all(isinstance(v, str) and len(v.encode()) <= 262144 for v in files.values()), 'observer raw field bound')
        require(all(isinstance(sample[key], str) and len(sample[key].encode()) <= 262144 for key in ('metrics','db') if key in sample),
                'observer HTTP field bound')
        require(files['stat'] == sample['process']['stat'] and files['status'] == sample['process']['status'] and
                files['cgroup'] == target['cgroup'], 'observer raw identity')
        duration = integer(sample['duration_ns'], True)
        begin, end = timestamp(sample['time']), timestamp(sample['end'])
        mono, mono_end = integer(sample['monotonic_ns'], True), integer(sample['end_monotonic_ns'], True)
        require(duration <= 2e9 and mono_end-mono == duration and abs(end-begin-duration/1e9) < .05, 'observer sample clocks')
        if previous:
            delta = (mono-previous[0])/1e9
            require(0 < delta <= options.get('max_gap_seconds', 6) and abs(begin-previous[1]-delta) < .1, 'observer clock/gap')
        previous = (mono, begin)
    if complete:
        terminal = entries[-1]
        require(set(terminal) == {'type','role','samples','ended_at'}, 'observer terminal fields')
        require(len(samples) == integer(terminal['samples']) == count and terminal['role'] == role, 'observer terminal count/role')
        require(0 <= timestamp(terminal['ended_at'])-timestamp(samples[-1]['end']) <= 2, 'observer terminal clock')
        seconds = (samples[-1]['monotonic_ns']-samples[0]['monotonic_ns'])/1e9
        require(options.get('minimum_seconds', options['seconds']-2) <= seconds <= options['seconds']+2, 'observer duration coverage')
        require(observer.parsed == len(observer.streams[0]) and
                all(len(line) <= 1 << 20 for line in observer.streams[0].splitlines()), 'observer trailing fragment/line bound')
    else:
        require(not all(observer.eof), 'missing observer terminal')
    return samples, complete


def finish_observation(observer, options, deadline=None):
    """Called after profile resource validation; EOF precedes Wait, never success."""
    receipt = dict(outcome='failed', role=observer.role)
    error = None
    try:
        _, complete = observation_samples(observer, options)
        require(complete, 'cannot acknowledge incomplete observer')
        require(observer.child.poll() is None and not observer.child.stdin.closed, 'observer exited before completion EOF')
        receipt['validated_monotonic'] = time.monotonic()
        path = observer.root/(observer.role+'-completion.json')
        path.write_text(json.dumps(receipt, indent=2)+'\n')
        observer.child.stdin.close()
        receipt['eof_sent_monotonic'] = time.monotonic()
        observer.stop(deadline=deadline)
        _, complete = observation_samples(observer, options)
        require(complete and observer.child.returncode == 0 and all(observer.eof) and observer.joined and
                observer.stopped is not None and all(p.closed for p in (observer.child.stdin, *observer.pipes)),
                'observer exit/EOF/Join/close')
        receipt.update(outcome='complete', waited_monotonic=time.monotonic(), exit=observer.child.returncode)
    except BaseException as exc:
        error = exc
        receipt['error'] = str(exc)
        try:
            abort_observation(observer, deadline)
        except BaseException as closing:
            exc.add_note('completion stop: '+str(closing))
        raise
    finally:
        try:
            (observer.root/(observer.role+'-completion.json')).write_text(json.dumps(receipt, indent=2)+'\n')
        except BaseException as recording:
            if error is None:
                raise
            error.add_note('completion record: '+str(recording))


def abort_observation(observer, deadline=None):
    """Cancel this qualification stream before Stop's EOF, even if polling failed."""
    error = None
    try:
        if observer.stop_requested is None and not observer.child.stdin.closed and observer.child.poll() is None:
            # Stdin has carried no data. One nonblocking byte rejects both an
            # ongoing sample and a terminal already waiting for its receipt.
            os.set_blocking(observer.child.stdin.fileno(), False)
            require(os.write(observer.child.stdin.fileno(), b'\x00') == 1, 'observer cancel write')
    except BaseException as exc:
        error = exc
        # If cancellation cannot be delivered, prevent Stop's EOF from becoming
        # a successful receipt. This is only the process owned by this Observer.
        try:
            observer.child.kill()
        except ProcessLookupError:
            pass
        except BaseException as killing:
            exc.add_note('observer cancel kill: '+str(killing))
    try:
        observer.stop(deadline=deadline)
    except BaseException as closing:
        if error is None:
            raise
        error.add_note('observer cancel stop: '+str(closing))
    if error is not None:
        raise error


def cancel_observation(observer, deadline):
    """Explicit early stop, recorded as cancellation with the helper's nonzero exit."""
    expected = observer.role+' observer exited: observer control byte received\n'
    try:
        abort_observation(observer, deadline)
    except RuntimeError as exc:
        require(str(exc) == expected and all(note == 'observer stop: '+expected for note in getattr(exc, '__notes__', [])),
                'unexpected observer cancellation failure: '+str(exc))
    else:
        raise ValueError('observer cancellation returned success')
    require(observer.child.returncode == 1 and observer.failure == expected and observer.joined and
            all(observer.eof) and observer.stopped is not None and
            all(p.closed for p in (observer.child.stdin, *observer.pipes)), 'observer cancellation Join/EOF/close')
    receipt = dict(outcome='cancelled-not-complete', exit=1, joined=True, eof=observer.eof,
                   stopped_monotonic=observer.stopped, samples=sum('files' in e for e in observer.entries))
    (observer.root/(observer.role+'-cancellation.json')).write_text(json.dumps(receipt, indent=2)+'\n')
