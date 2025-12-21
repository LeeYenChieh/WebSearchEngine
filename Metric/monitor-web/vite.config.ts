import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite'
import path from "path";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss(),],
  server: {
    port: 3000, // 這裡改成你想要的 port
    host: true, // 監聽所有網卡，讓局域網可訪問
    allowedHosts: true,
    fs: {
      allow: [
        path.resolve(__dirname, "..")   // 或直接寫絕對路徑
      ]
    }
  },
  resolve: {
    alias: {
      '@': '/src'
    }
  }
})
