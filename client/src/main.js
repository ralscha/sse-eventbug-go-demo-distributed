import App from './app.js';

const app = new App();
app.start();
window.addEventListener('pagehide', () => app.stop(), { once: true });
