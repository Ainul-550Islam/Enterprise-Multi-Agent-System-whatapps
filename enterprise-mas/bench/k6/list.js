// bench/k6/list.js
import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: {
    vus: 5,
    duration: "60s",
  },
  thresholds: {
    "http_req_duration": ["p(95)<800"],
    "http_req_failed": ["rate<0.1"],
  },
};

export default function () {
  const res = http.get("http://localhost:8080/executions", {
    headers: {
      "Authorization": "Bearer dev-token",
    },
  });

  check(res, {
    "status is 200": (r) => r.status === 200,
    "body is json": (r) => /^\{/i.test(r.body),
  });

  sleep(1);
}
