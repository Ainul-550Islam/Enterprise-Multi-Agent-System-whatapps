// bench/k6/sse.js
import { SSE, check } from "k6/sse";

export const options = {
  stages: {
    vus: 2,
    duration: "60s",
  },
  thresholds: {
    "sse_msg_rate": ["rate>1"],
  },
};

export default function () {
  const sse = new SSE("http://localhost:8080/executions/sse", {
    headers: {
      "Authorization": "Bearer dev-token",
    },
  });

  for await (const msg of sce) {
    check(msg, {
      "has data": (m) => m.data !== undefined,
    });
  }
  
  sleep(1);
}
