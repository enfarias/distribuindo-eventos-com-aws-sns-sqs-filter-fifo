import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Metricas customizadas: contagem total de reservas publicadas e tempo de resposta do endpoint.
const reservationsPublishedTotal = new Counter('reservation_published_total');
const reservationResponseTime = new Trend('reservation_response_time', true);

export const options = {
  stages: [
    { duration: '30s', target: 80 },
    { duration: '120s', target: 150 },
    { duration: '30s', target: 200 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.02'],
    http_req_duration: ['p(95)<5000'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';

// Distribui carga entre N shows para evidenciar paralelizacao por MessageGroupId
// (showId). Cada showId ordena suas reservas em FIFO, mas shows diferentes sao
// processados em paralelo ate 9.000 msg/s por grupo (High Throughput Mode).
const SHOW_IDS = [
  'show_taylor_2026_sao_paulo_2026_05_15',
  'show_taylor_2026_rio_2026_05_22',
  'show_coldplay_2026_sao_paulo_2026_06_10',
  'show_coldplay_2026_curitiba_2026_06_18',
  'show_artistax_2026_belo_horizonte_2026_07_05',
];
const TICKET_TIERS = ['VIP', 'CAMAROTE', 'PISTA', 'ARQUIBANCADA'];

export default function () {
  const correlationId = uuidv4();
  const reservationId = uuidv4();
  const showId = SHOW_IDS[Math.floor(Math.random() * SHOW_IDS.length)];
  const ticketTier = TICKET_TIERS[Math.floor(Math.random() * TICKET_TIERS.length)];
  const quantity = Math.floor(Math.random() * 6) + 1;
  const unitPriceUsd = (Math.random() * 400 + 100).toFixed(2);

  const payload = JSON.stringify({
    reservationId,
    showId,
    ticketTier,
    quantity,
    unitPriceUsd: parseFloat(unitPriceUsd),
    buyerEmail: `buyer_${reservationId.substring(0, 8)}@example.com`,
    buyerName: `Buyer ${reservationId.substring(0, 8)}`,
    requestedAt: new Date().toISOString(),
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Correlation-ID': correlationId,
    },
  };

  const res = http.post(`${BASE_URL}/api/reservations`, payload, params);

  reservationResponseTime.add(res.timings.duration);

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
    'response contains correlationId': (r) => r.body && r.body.includes('correlationId'),
  });

  if (ok) {
    reservationsPublishedTotal.add(1);
  }

  sleep(Math.random() * 0.3);
}
