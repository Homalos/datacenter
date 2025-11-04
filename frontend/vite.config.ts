import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [vue()],
  base: '/dashboard',  // ✅ 设置资源路径的 base，与 Vue Router base 一致
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src')
    }
  },
  server: {
    port: 5173,
    proxy: {
      '/datacenter': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true
      },
      '/health': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true
      },
      '/metrics': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true
      },
      '/docs': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true
      },
      '/openapi.json': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true
      },
      '/redoc': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true
      },
      '/about': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true
      }
    }
  },
  build: {
    outDir: '../static',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: 'dashboard.js',
        chunkFileNames: 'chunks/[name]-[hash].js',
        assetFileNames: (assetInfo) => {
          if (assetInfo.name === 'index.css') {
            return 'dashboard.css'
          }
          return 'assets/[name]-[hash].[ext]'
        }
      }
    }
  }
})

