// bench/k6/submit.js
import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: {
    vus: 10,
    duration: "30s",
  },
  thresholds: {
    "http_req_duration": ["p(95)<500"],
    "http_req_failed": ["rate<0.1"],
  },
};

export default function () {
  const res = http.post(
    "http://localhost:8080/executions",
    JSON.stringify({
      workflow_id: "approval-flow",
      input: { question: "What is the capital of France?" },
    }),
    {
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer dev-token",
      },
    }),
  );

  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  sleep(1);
}
