import Fastify from 'fastify';
import cors from '@fastify/cors';
import brasilRoutes from './routes/brasil';
import healthRoutes from './routes/health';
import peruRoutes from './routes/peru';
import chileRoutes from './routes/chile';
import usersRoutes from './routes/users';
import authRoutes from './routes/auth';

export function buildApp() {
  const app = Fastify({ logger: true });
  // Habilitar CORS com configuração padrão segura
  app.register(cors, {
    origin: true, // permite qualquer origem; ajuste para lista específica se necessário
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true,
  });
  app.register(brasilRoutes, { prefix: '/brasil' });
  app.register(peruRoutes, { prefix: '/peru' });
  app.register(chileRoutes, { prefix: '/chile' });
  app.register(usersRoutes, { prefix: '/usuarios' });
  app.register(authRoutes, { prefix: '/auth' });
  app.register(healthRoutes);
  return app;
}

export default buildApp;