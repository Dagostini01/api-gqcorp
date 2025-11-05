import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { PrismaClient } from '@prisma/client';
import { queryRoboComex } from '../services/brasilService';
import { DataTransformer } from '../database/data-transformer';

interface RoboComexBody {
  ncm: string;
  data_de: string;
  data_ate: string;
  limit?: number;
}

interface BrasilQuery {
  page?: number;
  pageSize?: number;
  ncm?: string;
}

const brasilRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  // GET /brasil -> lista importações do Brasil com paginação e filtro por NCM
  app.get<{ Querystring: BrasilQuery }>('/', {
    schema: {
      querystring: {
        type: 'object',
        properties: {
          page: { type: 'integer', minimum: 1 },
          pageSize: { type: 'integer', minimum: 1, maximum: 100 },
          ncm: { type: 'string' },
        },
        additionalProperties: true,
      }
    }
  }, async (request, reply) => {
    const prisma = new PrismaClient();
    try {
      const { page = 1, pageSize = 20, ncm } = request.query || {};
      const take = Math.min(100, Math.max(1, Number(pageSize) || 20));
      const pageNum = Math.max(1, Number(page) || 1);
      const skip = (pageNum - 1) * take;

      const where: any = { country: { code: 'BR' } };
      if (ncm && typeof ncm === 'string' && ncm.trim() !== '') {
        where.product = { code: ncm.trim() };
      }

      const total = await prisma.import.count({ where });

      const imports = await prisma.import.findMany({
        where,
        orderBy: { id: 'desc' },
        skip,
        take,
        include: {
          country: { select: { code: true } },
          state: { select: { code: true, name: true } },
          product: { select: { code: true, commercialDesc: true, description: true } },
          company: { select: { document: true, name: true } },
          originCountry: { select: { name: true } },
        },
      });

      // Helper: converte "01/mm/yyyy" para ISO "yyyy-mm-01T00:00:00.000Z"
      const fecToISO = (fec?: string | null): string | null => {
        if (!fec) return null;
        const m = fec.match(/^\s*(\d{2})\/(\d{2})\/(\d{4})\s*$/);
        if (!m) return null;
        const yyyy = parseInt(m[3], 10);
        const mm = parseInt(m[2], 10);
        if (isNaN(yyyy) || isNaN(mm) || mm < 1 || mm > 12) return null;
        const d = new Date(yyyy, mm - 1, 1);
        return d.toISOString();
      };

      // Mapear para formato próximo ao JSON do robô, com números e fallbacks
      const resultados = imports.map((imp) => ({
        country_code: imp.country.code,
        importador: imp.company.document,
        declaracao: imp.declarationNumber,
        serie: (imp.series ?? (imp as any).rawData?.serie ?? null),
        partida: imp.product.code,
        fecNumeracao: imp.numerationDate ?? null,
        paisOrig: imp.originCountry?.name ?? null,
        state: (imp.state?.name ?? (imp as any).rawData?.state ?? null),
        descComer: imp.product.commercialDesc ?? imp.product.description ?? null,
        fobUsd: imp.fobUsd != null ? Number(imp.fobUsd as any) : null,
        fleteUsd: imp.freightUsd != null ? Number(imp.freightUsd as any) : null,
        seguro: imp.insuranceUsd != null ? Number(imp.insuranceUsd as any) : null,
        cif: imp.cifUsd != null ? Number(imp.cifUsd as any) : null,
        pesoNeto: imp.netWeight != null ? Number(imp.netWeight as any) : null,
        // Extras úteis
        operationDate: (imp.operationDate ? imp.operationDate.toISOString() : fecToISO(imp.numerationDate)),
        unit: imp.unit ?? null,
        createdAt: imp.createdAt,
        id: imp.id,
      }));

      await prisma.$disconnect();
      reply.header('Content-Type', 'application/json; charset=utf-8');
      return reply.code(200).send({
        total,
        page: pageNum,
        pageSize: take,
        totalPages: Math.max(1, Math.ceil(total / take)),
        resultados,
      });
    } catch (err: any) {
      await prisma.$disconnect();
      request.log.error({ err }, 'Falha ao listar imports do Brasil');
      return reply.code(500).send({ error: 'list_brasil_failed', detail: err?.message });
    }
  });

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
      // Verificação: se o período já está cadastrado para o(s) NCM(s), não permitir novo insert
      const prisma = new PrismaClient();
      const ncmList = (typeof ncm === 'string' ? ncm.split(/[\s,]+/).filter(Boolean) : []).map(s => s.trim());
      // Parse YYYY-MM -> Date (primeiro dia do mês)
      const parseYYYYMM = (s: string): Date | null => {
        const m = s?.match(/^\s*(\d{4})-(\d{2})\s*$/);
        if (!m) return null;
        const yyyy = parseInt(m[1], 10);
        const mm = parseInt(m[2], 10);
        if (isNaN(yyyy) || isNaN(mm) || mm < 1 || mm > 12) return null;
        return new Date(yyyy, mm - 1, 1);
      };
      const startDate = parseYYYYMM(data_de);
      const endDate = parseYYYYMM(data_ate);
      if (!startDate || !endDate) {
        await prisma.$disconnect();
        return reply.code(400).send({ error: 'invalid_period', detail: 'data_de/data_ate devem estar no formato YYYY-MM' });
      }

      const whereDup: any = {
        country: { code: 'BR' },
        operationDate: { gte: startDate, lte: endDate },
      };
      if (ncmList.length > 0) {
        whereDup.product = { code: { in: ncmList } };
      }

      const existingCount = await prisma.import.count({ where: whereDup });
      await prisma.$disconnect();
      if (existingCount > 0) {
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(409).send({
          error: 'period_already_imported',
          detail: 'Já existem registros para o período informado no Brasil',
          ncm: ncmList,
          periodo: { de: data_de, ate: data_ate },
          existingCount,
        });
      }

      const data = await queryRoboComex(ncm, data_de, data_ate);
      // Preparar resposta mínima e selecionar registros a processar
      let registros: any[] = Array.isArray(data?.resultados) ? (data.resultados as any[]) : [];
      if (typeof limit === 'number' && Number.isFinite(limit) && limit > 0) {
        registros = registros.slice(0, limit);
      }
      const descricaoBase = typeof data.descricao === 'string' ? data.descricao : '';
      const descricao = (typeof limit === 'number' && registros.length > 0)
        ? (descricaoBase ? `${descricaoBase} (limitado a ${registros.length})` : `limitado a ${registros.length}`)
        : descricaoBase;
      // Garantir country_code nos registros (fallback para 'BR') somente para processamento
      registros = registros.map((r: any) => ({
        ...r,
        country_code: r.country_code || 'BR',
      }));

      // Persistir no banco via DataTransformer
      const transformer = new DataTransformer();
      // Garante países/estados básicos criados para evitar "País não encontrado: BR"
      await transformer.initializeBaseData();
      await transformer.processRobotResponse({
        descricao,
        total: registros.length,
        resultados: registros,
      });
      await transformer.disconnect();

      reply.header('Content-Type', 'application/json; charset=utf-8');
      return reply.code(200).send({ total: registros.length, descricao, persisted: true });
    } catch (err: any) {
      request.log.error({ err }, 'Falha ao consultar robo_comex');
      return reply.code(500).send({ error: 'robo_comex_failed', detail: err?.message });
    }
  });
};

export default brasilRoutes;