/** @type {import("tailwindcss").Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#0b1220",
        panel: "#111a2e",
        border: "#1f2a44",
        fg: "#e6edf7",
        muted: "#8ea0b8",
        accent: "#ff7a59",
        accent2: "#6ea8ff",
        ok: "#3dd68c",
        warn: "#ffb347",
        bad: "#ff6b6b",
      },
      fontFamily: {
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
    },
  },
  plugins: [],
};
