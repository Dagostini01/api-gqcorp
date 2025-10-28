import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { queryChileImport } from '../services/chileService';

interface ChileBody {
  ano: number;
  mes: number; // 1..12
  limit?: number; // opcional
}

const chileRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  app.post<{ Body: ChileBody }>('/importar', {
    schema: {
      body: {
        type: 'object',
        properties: {
          ano: { type: 'integer', minimum: 1900, maximum: 2100 },
          mes: { type: 'integer', minimum: 1, maximum: 12 },
          limit: { type: 'integer', minimum: 1 },
        },
        required: ['ano', 'mes'],
        additionalProperties: false,
      },
    },
  }, async (request, reply) => {
    const { ano, mes, limit } = request.body;
    const json = await queryChileImport(ano, mes, limit);
    reply.header('Content-Type', 'application/json; charset=utf-8');
    return json;
  });
};

export default chileRoutes;