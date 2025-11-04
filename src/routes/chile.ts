import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { queryChileImport } from '../services/chileService';
import { DataTransformer } from '../database/data-transformer';
import { PrismaClient, Prisma } from '@prisma/client';

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
    // Consultar o robô Python
    const raw = await queryChileImport(ano, mes, limit);

    // Garantir country_code compatível com a base ('CL') e preparar para persistência
    const resultados = Array.isArray(raw?.resultados) ? raw.resultados : [];
    const resultadosNormalizados = resultados.map((r: any) => ({
      ...r,
      country_code: 'CL',
    }));

    // Persistir em background (transformação e gravação)
    const transformer = new DataTransformer();
    await transformer.initializeBaseData();
    await transformer.processRobotResponse({
      descricao: raw?.descricao ?? `Importações do Chile ${ano}-${mes}`,
      total: resultadosNormalizados.length,
      resultados: resultadosNormalizados,
    });
    await transformer.disconnect();

    // Retornar o mesmo formato do robô (para compatibilidade com o cliente)
    reply.header('Content-Type', 'application/json; charset=utf-8');
    return {
      descricao: raw?.descricao ?? `Importações do Chile ${ano}-${mes}`,
      total: resultados.length,
      resultados,
    };
  });

  // GET /chile/importar -> retorna JSON igual ao POST, mas vindo do BD (paginação/opcional limit)
  app.get('/importar', {
    schema: {
      querystring: {
        type: 'object',
        properties: {
          ano: { type: 'integer', minimum: 1900, maximum: 2100 },
          mes: { type: 'integer', minimum: 1, maximum: 12 },
          limit: { type: 'integer', minimum: 1 },
          page: { type: 'integer', minimum: 1 },
          pageSize: { type: 'integer', minimum: 1, maximum: 200 },
        },
        required: ['ano', 'mes'],
        additionalProperties: false,
      },
    },
  }, async (request, reply) => {
    const { ano, mes, limit, page, pageSize } = request.query as any;
    const prisma = new PrismaClient();
    try {
      // País Chile
      const country = await prisma.country.findUnique({ where: { code: 'CL' } });
      if (!country) {
        await prisma.$disconnect();
        return reply.code(404).send({ error: 'country_not_found', detail: 'CL' });
      }

      // Período
      const start = new Date(Number(ano), Number(mes) - 1, 1);
      const end = new Date(Number(ano), Number(mes), 0);

      // Buscar imports com rawData salvo
      const pageNum = (typeof page === 'number' && page > 0) ? Math.floor(page) : 1;
      const takeDefault = (typeof pageSize === 'number' && pageSize > 0 && pageSize <= 200) ? Math.floor(pageSize) : 20;
      const take = (typeof limit === 'number' && limit > 0) ? Math.min(Math.floor(limit), takeDefault) : takeDefault;
      const skip = (pageNum - 1) * take;

      const where = {
        countryId: country.id,
        operationDate: { gte: start, lte: end },
        rawData: { not: Prisma.DbNull },
      } as const;

      const periodTotal = await prisma.import.count({ where });

      const imports = await prisma.import.findMany({
        where,
        take,
        skip,
        orderBy: { id: 'desc' },
      });

      const resultados = imports.map((imp) => {
        const rawData = (imp as any).rawData || {};
        // Garantir campos auxiliares
        return {
          ...rawData,
          country_code: 'CL',
          ano_ref: String((rawData as any).ano_ref ?? ano),
          mes_ref: String((rawData as any).mes_ref ?? mes),
        };
      });
      const total = periodTotal;

      await prisma.$disconnect();

      // Montar descrição igual ao robô
      const firstDay = `${String(ano)}-${String(mes).padStart(2, '0')}-01`;
      const lastDay = `${String(ano)}-${String(mes).padStart(2, '0')}-${String(end.getDate()).padStart(2, '0')}`;
      const limTag = (typeof limit === 'number') ? ` (limitado a ${limit})` : '';
      const descricao = `Foram encontradas ${total} importações no período de ${firstDay} a ${lastDay}${limTag}`;

      const totalPages = Math.max(1, Math.ceil(total / take));

      reply.header('Content-Type', 'application/json; charset=utf-8');
      return {
        descricao,
        total,
        page: pageNum,
        pageSize: take,
        totalPages,
        resultados,
      };
    } catch (err: any) {
      await prisma.$disconnect();
      request.log.error({ err }, 'Falha ao listar imports do Chile');
      return reply.code(500).send({ error: 'list_chile_failed', detail: err?.message });
    }
  });
};

export default chileRoutes;