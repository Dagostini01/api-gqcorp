import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { queryRoboComex } from '../services/brasilService';

interface RoboComexBody {
  ncm: string;
  data_de: string;
  data_ate: string;
  limit?: number;
}

const brasilRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  app.post<{ Body: RoboComexBody }>('/importar', {
    schema: {
      body: {
        type: 'object',
        properties: {
          ncm: { type: 'string' },
          data_de: { type: 'string' },
          data_ate: { type: 'string' },
          limit: { type: 'integer', minimum: 1 },
        },
        required: ['ncm', 'data_de', 'data_ate'],
        additionalProperties: false,
      },
    },
  }, async (request, reply) => {
    const { ncm, data_de, data_ate, limit } = request.body;
    try {
      const data = await queryRoboComex(ncm, data_de, data_ate);
      let response = data;
      if (typeof limit === 'number' && Array.isArray(data?.resultados)) {
        const recs = data.resultados as any[];
        const sliced = recs.slice(0, limit);
        const limited = recs.length > sliced.length;
        const descricaoBase = typeof data.descricao === 'string' ? data.descricao : '';
        response = {
          ...data,
          total: sliced.length,
          resultados: sliced,
          descricao: limited ? (descricaoBase ? `${descricaoBase} (limitado a ${limit})` : `limitado a ${limit}`) : descricaoBase,
        };
      }
      reply.header('Content-Type', 'application/json; charset=utf-8');
      return reply.code(200).send(response);
    } catch (err: any) {
      request.log.error({ err }, 'Falha ao consultar robo_comex');
      return reply.code(500).send({ error: 'robo_comex_failed', detail: err?.message });
    }
  });
};

export default brasilRoutes;