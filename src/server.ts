import { buildApp } from './app';
import { PORT } from './config/env';

const app = buildApp();

app
  .listen({ port: PORT, host: '0.0.0.0' })
  .then(() => app.log.info(`API rodando em http://localhost:${PORT}`))
  .catch((err) => {
    app.log.error(err);
    process.exit(1);
  });