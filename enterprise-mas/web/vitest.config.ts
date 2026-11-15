import { defineConfig } from "vitest/config"
import react from "@vitejs/plugin-react"

export default defineConfig({
  plugins: [react()],
  test: {
    globals: false,
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
    cache: ["src"],
  },
  exclude: ["**/e2e/**", "**/node_modules/**"],
})
