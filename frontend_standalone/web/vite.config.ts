import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Vite 7 + React 19 SPA scaffold for refactored/frontend_standalone/web/
// ([vite build](vite) emits to dist/ -- consumed by refactored/frontend_standalone/scripts/build-and-bundle.sh).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: false,
  },
})
