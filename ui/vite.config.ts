import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { resolve } from "path";

/** Redirect / → /ui/ in dev so the default URL works. */
function devRedirect(): Plugin {
  return {
    name: "dev-redirect",
    configureServer(server) {
      server.middlewares.use((req, _res, next) => {
        if (req.url === "/") {
          req.url = "/ui/";
        }
        next();
      });
    },
  };
}

export default defineConfig({
  plugins: [devRedirect(), react(), tailwindcss()],
  base: "/ui/",
  resolve: {
    alias: {
      "@": resolve(__dirname, "src"),
    },
  },
  server: {
    proxy: {
      "/api": "http://localhost:8000",
    },
  },
});
