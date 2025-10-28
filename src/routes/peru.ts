import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { queryAduanetPeru } from '../services/peruService';

interface PeruBody {
  cnpj: string;
  data_de: string; // YYYY-MM-DD
  data_ate: string; // YYYY-MM-DD
  limit?: number;
}

const peruRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  app.post<{ Body: PeruBody }>('/importar', {
    schema: {
      body: {
        type: 'object',
        properties: {
          cnpj: { type: 'string' },
          data_de: { type: 'string' },
          data_ate: { type: 'string' },
          limit: { type: 'integer', minimum: 1 },
        },
        required: ['cnpj', 'data_de', 'data_ate'],
        additionalProperties: false,
      },
    },
  }, async (request, reply) => {
    const { cnpj, data_de, data_ate, limit } = request.body;
    try {
      const data = await queryAduanetPeru(data_de, data_ate, cnpj);
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
      request.log.error({ err }, 'Falha ao consultar robo_aduanet (Peru)');
      return reply.code(500).send({ error: 'robo_aduanet_failed', detail: err?.message });
    }
  });
};

export default peruRoutes;