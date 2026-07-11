import { defineConfig } from 'vite';

export default defineConfig({
    server: {
        host: true,
        port: 5173,
        proxy: {
            '/node-a': {
                target: 'http://localhost:8080',
                rewrite: (path) => path.replace(/^\/node-a/, '')
            },
            '/node-b': {
                target: 'http://localhost:8081',
                rewrite: (path) => path.replace(/^\/node-b/, '')
            }
        }
    },
    build: {
        outDir: '../src/main/resources/static',
        emptyOutDir: true
    }
});
