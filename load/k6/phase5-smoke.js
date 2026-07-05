import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

export const options = {
  vus: Number(__ENV.VUS || 2),
  duration: __ENV.DURATION || '60s',
  thresholds: {
    checks: ['rate>0.95'],
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<2000']
  }
};

const baseURL = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const token = __ENV.TOKEN || __ENV.PULSEQUEUE_OPERATOR_TOKEN || 'change-me';

function authHeaders() {
  return {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json'
  };
}

export default function () {
  const live = http.get(`${baseURL}/health/live`);
  check(live, {
    'live health is 200': (res) => res.status === 200
  });

  const ready = http.get(`${baseURL}/health/ready`);
  check(ready, {
    'ready health is 200': (res) => res.status === 200
  });

  const key = `phase5-k6-${Date.now()}-${exec.vu.idInTest}-${exec.scenario.iterationInTest}`;
  const payload = JSON.stringify({
    queue: 'default',
    type: 'demo.echo',
    payload: {
      message: 'phase5 k6 smoke'
    },
    max_attempts: 1,
    idempotency_key: key
  });
  const submit = http.post(`${baseURL}/jobs`, payload, { headers: authHeaders() });
  check(submit, {
    'job submit is 201': (res) => res.status === 201
  });

  let jobID = '';
  try {
    jobID = submit.json('job.id') || '';
  } catch (_) {
    jobID = '';
  }

  if (jobID) {
    let terminal = false;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      sleep(1);
      const status = http.get(`${baseURL}/jobs/${jobID}`, { headers: authHeaders() });
      terminal = check(status, {
        'job status fetch is 200': (res) => res.status === 200
      }) && ['succeeded', 'dead_letter', 'cancelled'].includes(status.json('job.status'));
      if (terminal) {
        check(status, {
          'job succeeds during smoke': (res) => res.json('job.status') === 'succeeded'
        });
        break;
      }
    }
    check(terminal, {
      'job reaches terminal state': (value) => value === true
    });
  }

  const queues = http.get(`${baseURL}/queues`, { headers: authHeaders() });
  check(queues, {
    'queue inspection is 200': (res) => res.status === 200
  });

  sleep(1);
}
