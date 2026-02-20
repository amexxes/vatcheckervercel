import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Proxy in dev so your API calls go to localhost:3000 (your Express server)
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://localhost:3000"
    }
  }
});
