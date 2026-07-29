// Runtime configuration (optional). Override by bind-mounting a replacement
// at `/usr/share/nginx/html/config.js` in the Docker image, or by editing
// this file before serving the `dist/` output statically.
//
// Anything you set here overrides the bundled defaults from
// `src/lib/config.ts`; it does NOT override per-user settings saved in
// localStorage.
//
// Schema:
//   window.__MASSA_EXPLORER_CONFIG__ = {
//     defaultNetwork: "mainnet" | "buildnet",
//     endpoints: {
//       mainnet:  ["https://indexer-a.example.com", "https://indexer-b.example.com"],
//       buildnet: ["https://buildnet-indexer.example.com"],
//     },
//   };
window.__MASSA_EXPLORER_CONFIG__ = window.__MASSA_EXPLORER_CONFIG__ || {};
