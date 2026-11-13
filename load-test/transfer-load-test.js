import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// ── Custom Metrics ────────────────────────────────────────────
const transferErrors = new Counter('transfer_errors');
const transferSuccess = new Rate('transfer_success_rate');
const transferDuration = new Trend('transfer_duration', true);

// ── Options ───────────────────────────────────────────────────
export const options = {
  scenarios: {
    smoke: {
      executor: 'constant-vus',
      vus: 5,
      duration: '30s',
      tags: { scenario: 'smoke' },
    },
    load: {
      executor: 'constant-vus',
      vus: 50,
      duration: '2m',
      startTime: '35s',
      tags: { scenario: 'load' },
    },
    stress: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '20s', target: 200 },
        { duration: '20s', target: 200 },
        { duration: '20s', target: 0 },
      ],
      startTime: '2m40s',
      tags: { scenario: 'stress' },
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<200', 'p(99)<500'],
    http_req_failed: ['rate<0.01'],
    transfer_success_rate: ['rate>0.99'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8088';
const SOURCE_ACCOUNT_ID = __ENV.SRC_ACCOUNT || 'a0000000-0000-0000-0000-000000000001';
const TARGET_ACCOUNT_ID = __ENV.TGT_ACCOUNT || 'b0000000-0000-0000-0000-000000000002';

// ── Health Check ──────────────────────────────────────────────
export function setup() {
  const healthRes = http.get(`${BASE_URL}/health`, {
    tags: { name: 'HealthCheck' },
  });
  check(healthRes, {
    'health endpoint returns 200': (r) => r.status === 200,
  });
}

// ── Main Scenario ─────────────────────────────────────────────
export default function () {
  const idempotencyKey = uuidv4();

  const payload = JSON.stringify({
    sourceAccountId: SOURCE_ACCOUNT_ID,
    targetAccountId: TARGET_ACCOUNT_ID,
    amount: (Math.random() * 50 + 10).toFixed(2),
    currency: 'INR',
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey,
    },
    tags: { name: 'CreateTransfer' },
  };

  group('Transfer Flow', () => {
    // ── Create Transfer ─────────────────────────────────────
    const res = http.post(`${BASE_URL}/api/v1/transfers`, payload, params);

    transferDuration.add(res.timings.duration);

    const ok = check(res, {
      'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
    });

    if (ok) {
      transferSuccess.add(1);
    } else {
      transferSuccess.add(0);
      transferErrors.add(1);
    }

    // ── Idempotency Replay (10% of requests) ────────────────
    if (Math.random() < 0.1) {
      const replayParams = {
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': idempotencyKey,
        },
        tags: { name: 'IdempotentReplay' },
      };

      const replayRes = http.post(`${BASE_URL}/api/v1/transfers`, payload, replayParams);

      check(replayRes, {
        'replay returns 200 or 409 (cached)': (r) => r.status === 200 || r.status === 409,
      });
    }
  });

  sleep(0.5);
}
