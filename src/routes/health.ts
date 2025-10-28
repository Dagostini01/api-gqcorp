import { FastifyInstance, FastifyPluginAsync } from 'fastify';

const healthRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  app.get('/health', async () => {
    return {
      status: 'ok',
      time: new Date().toISOString(),
      service: 'api-gqcorp',
      version: '0.1.0'
    };
  });
};

export default healthRoutes;