"""
Watch a budgetkey pipeline until a run of NEW code reaches a terminal state.

    python3 watch.py activities/social_services/measurements/collect \
        [--baseline-flow-hash HASH] [--baseline-ended EPOCH] \
        [--poll 60] [--cap-minutes 180]

Emits one line per state change and exits on the first terminal state of a run
whose flow hash differs from the baseline -- success OR failure, so silence is
never mistaken for success. Exit code 0 succeeded, 1 failed, 2 timed out.

Intended to be run through the Monitor tool with persistent: true.
"""
import argparse
import json
import sys
import time
import urllib.request

API = 'https://pipelines.obudget.org/api/raw/%s'
TERMINAL_EXCLUDED = ('RUNNING', 'QUEUED', 'INIT', None)


def fetch(pipeline_id):
    req = urllib.request.Request(API % pipeline_id, headers={'User-Agent': 'curl/8'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def summarise(d):
    flow_hash = None
    for step in d.get('pipeline', {}).get('pipeline', []):
        if step.get('run') == 'flow':
            flow_hash = step.get('parameters', {}).get('__flow_hash')
    return {
        'flow_hash': flow_hash,
        'state': d.get('state'),
        'message': d.get('message'),
        'success': d.get('success'),
        'dirty': d.get('dirty'),
        'ended': d.get('ended') or 0,
        'rows': (d.get('stats') or {}).get('count_of_rows'),
        'errors': len(d.get('error_log') or []),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('pipeline_id')
    ap.add_argument('--baseline-flow-hash')
    ap.add_argument('--baseline-ended', type=float)
    ap.add_argument('--poll', type=int, default=60)
    ap.add_argument('--cap-minutes', type=int, default=180)
    args = ap.parse_args()

    base_hash, base_ended = args.baseline_flow_hash, args.baseline_ended
    deadline = time.time() + args.cap_minutes * 60
    prev = None

    while time.time() < deadline:
        try:
            s = summarise(fetch(args.pipeline_id))
        except Exception as e:                       # transient API failure
            print('[%s] poll failed: %s' % (time.strftime('%H:%M:%S'), e))
            sys.stdout.flush()
            time.sleep(args.poll)
            continue

        if base_hash is None:                        # baseline from first poll
            base_hash, base_ended = s['flow_hash'], s['ended']
            print('[%s] baseline flow_hash=%s ended=%s rows=%s'
                  % (time.strftime('%H:%M:%S'), base_hash, base_ended, s['rows']))
            sys.stdout.flush()
        if base_ended is None:
            base_ended = 0

        key = (s['flow_hash'], s['state'], s['message'], s['dirty'], s['ended'])
        if key != prev:
            print('[%s] code=%s state=%s msg=%r dirty=%s rows=%s errors=%d'
                  % (time.strftime('%H:%M:%S'),
                     'NEW' if s['flow_hash'] != base_hash else 'old',
                     s['state'], s['message'], s['dirty'], s['rows'], s['errors']))
            sys.stdout.flush()
        prev = key

        new_code = s['flow_hash'] != base_hash
        terminal = s['state'] not in TERMINAL_EXCLUDED
        if new_code and terminal and s['ended'] > base_ended:
            verdict = 'SUCCEEDED' if s['success'] else 'FAILED'
            print('DONE %s -- new code ran, %s rows, %d errors'
                  % (verdict, s['rows'], s['errors']))
            sys.stdout.flush()
            return 0 if s['success'] else 1

        time.sleep(args.poll)

    print('DONE TIMEOUT -- no run of new code after %d minutes' % args.cap_minutes)
    return 2


if __name__ == '__main__':
    sys.exit(main())
