import Fastify from 'fastify';
import brasilRoutes from './routes/brasil';
import healthRoutes from './routes/health';
import peruRoutes from './routes/peru';
import chileRoutes from './routes/chile';

export function buildApp() {
  const app = Fastify({ logger: true });
  app.register(brasilRoutes, { prefix: '/brasil' });
  app.register(peruRoutes, { prefix: '/peru' });
  app.register(chileRoutes, { prefix: '/chile' });
  app.register(healthRoutes);
  return app;
}

export default buildApp;