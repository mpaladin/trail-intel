import { defineConfig } from "vite";

export default defineConfig({
  base: "./",
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
    globals: true,
  },
});
