import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        canvas: "var(--bg)",
        panel: "var(--panel)",
        ink: "var(--text)",
        muted: "var(--muted)",
        accent: "var(--accent)",
        ok: "var(--ok)",
        warn: "var(--warn)",
        danger: "var(--danger)",
        line: "var(--border)",
      },
    },
  },
  plugins: [],
};

export default config;
