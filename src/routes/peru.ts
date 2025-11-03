import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { queryAduanetPeru } from '../services/peruService';
import { PrismaClient } from '@prisma/client';
import { DataTransformer } from '../database/data-transformer';

interface PeruBody {
  cnpj: string;
  data_de: string; // YYYY-MM-DD
  data_ate: string; // YYYY-MM-DD
  limit?: number;
  force?: boolean;
}

interface PeruQuery {
  page?: number;
  pageSize?: number;
  cnpj?: string;
}

const peruRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  // GET /peru -> lista importações do Peru com paginação e filtro por CNPJ
  app.get<{ Querystring: PeruQuery }>('/', {
    schema: {
      querystring: {
        type: 'object',
        properties: {
          page: { type: 'integer', minimum: 1 },
          pageSize: { type: 'integer', minimum: 1, maximum: 100 },
          cnpj: { type: 'string' },
        },
        additionalProperties: true,
      }
    }
  }, async (request, reply) => {
    const prisma = new PrismaClient();
    try {
      const { page = 1, pageSize = 20, cnpj } = request.query || {};
      const take = Math.min(100, Math.max(1, Number(pageSize) || 20));
      const pageNum = Math.max(1, Number(page) || 1);
      const skip = (pageNum - 1) * take;

      const where: any = { country: { code: 'PE' } };
      if (cnpj && typeof cnpj === 'string' && cnpj.trim() !== '') {
        where.company = { document: cnpj.trim() };
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
          originCountry: { select: { code: true, name: true } },
          acquisitionCountry: { select: { code: true, name: true } },
          agency: { select: { code: true, name: true } },
        },
      });

      // Helper: converte "dd/mm/yyyy" para ISO (usa dia 1 do mês quando aplicável)
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

      const resultados = imports.map((imp) => ({
        // Campos base
        country_code: imp.country.code,
        importador: imp.company.document,
        declaracao: imp.declarationNumber,
        serie: (imp.series ?? (imp as any).rawData?.serie ?? null),
        partida: imp.product.code,
        fecNumeracao: imp.numerationDate ?? null,
        state: (imp.state?.name ?? imp.state?.code ?? (imp as any).rawData?.state ?? null),

        // Descrições do produto e complementares (Peru)
        descComer: imp.product.commercialDesc ?? imp.product.description ?? null,
        descPresent: (imp as any).presentationDesc ?? null,
        descMatConst: (imp as any).materialDesc ?? null,
        descUso: (imp as any).useDesc ?? null,
        descOutros: (imp as any).othersDesc ?? null,

        // Países de origem e aquisição (preferindo código do país)
        paisOrig: imp.originCountry?.code ?? null,
        paisOrigName: imp.originCountry?.name ?? null,
        paisAdq: imp.acquisitionCountry?.code ?? null,
        paisAdqName: imp.acquisitionCountry?.name ?? null,

        // Valores financeiros
        fobUsd: imp.fobUsd != null ? Number(imp.fobUsd as any) : null,
        fleteUsd: imp.freightUsd != null ? Number(imp.freightUsd as any) : null,
        seguro: imp.insuranceUsd != null ? Number(imp.insuranceUsd as any) : null,
        cif: imp.cifUsd != null ? Number(imp.cifUsd as any) : null,
        fob: imp.fobLocal != null ? Number(imp.fobLocal as any) : null,
        flete: imp.freightLocal != null ? Number(imp.freightLocal as any) : null,
        seguro2: imp.insuranceLocal != null ? Number(imp.insuranceLocal as any) : null,

        // Impostos e taxas
        adv: imp.adValorem != null ? Number(imp.adValorem as any) : null,
        igv: imp.igv != null ? Number(imp.igv as any) : null,
        isc: imp.isc != null ? Number(imp.isc as any) : null,
        ipm: imp.ipm != null ? Number(imp.ipm as any) : null,
        derEsp: imp.specialRights != null ? Number(imp.specialRights as any) : null,
        derAnt: imp.previousRights != null ? Number(imp.previousRights as any) : null,
        ipmAdic: imp.additionalIpm != null ? Number(imp.additionalIpm as any) : null,

        // Pesos, quantidades, volumes
        pesoNeto: imp.netWeight != null ? Number(imp.netWeight as any) : null,
        pesoNeto2: imp.netWeight2 != null ? Number(imp.netWeight2 as any) : null,
        quantidade: imp.quantity != null ? Number(imp.quantity as any) : null,
        nroBultos: imp.packages ?? null,
        unid: imp.unit ?? null,
        unit: imp.unit ?? null,

        // Controle
        canal: (imp as any).channel ?? null,
        armazen: (imp as any).warehouse ?? null,
        commod: (imp as any).commodity ?? null,
        agencia: imp.agency?.code ?? null,

        // Datas
        operationDate: (imp.operationDate ? imp.operationDate.toISOString() : fecToISO(imp.numerationDate)),

        // Metadados
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
      request.log.error({ err }, 'Falha ao listar imports do Peru');
      return reply.code(500).send({ error: 'list_peru_failed', detail: err?.message });
    }
  });
  app.post<{ Body: PeruBody }>('/importar', {
    schema: {
      body: {
        type: 'object',
        properties: {
          cnpj: { type: 'string' },
          data_de: { type: 'string' },
          data_ate: { type: 'string' },
          limit: { type: 'integer', minimum: 1 },
          force: { type: 'boolean' },
        },
        required: ['cnpj', 'data_de', 'data_ate'],
        additionalProperties: false,
      },
    },
  }, async (request, reply) => {
    const { cnpj, data_de, data_ate, limit, force } = request.body;
    try {
      // Validação de período já importado (igual ao Brasil, mas para PE)
      const prisma = new PrismaClient();
      const parseYYYYMMDD = (s: string): Date | null => {
        const m = s?.match(/^\s*(\d{4})-(\d{2})-(\d{2})\s*$/);
        if (!m) return null;
        const yyyy = parseInt(m[1], 10);
        const mm = parseInt(m[2], 10);
        const dd = parseInt(m[3], 10);
        if (isNaN(yyyy) || isNaN(mm) || isNaN(dd) || mm < 1 || mm > 12 || dd < 1 || dd > 31) return null;
        return new Date(yyyy, mm - 1, dd);
      };
      const startDate = parseYYYYMMDD(data_de);
      const endDate = parseYYYYMMDD(data_ate);
      if (!startDate || !endDate) {
        await prisma.$disconnect();
        return reply.code(400).send({ error: 'invalid_period', detail: 'data_de/data_ate devem estar no formato YYYY-MM-DD' });
      }

      if (!force) {
        const existingCount = await prisma.import.count({
          where: {
            country: { code: 'PE' },
            operationDate: { gte: startDate, lte: endDate },
          },
        });
        await prisma.$disconnect();
        if (existingCount > 0) {
          reply.header('Content-Type', 'application/json; charset=utf-8');
          return reply.code(409).send({
            error: 'period_already_imported',
            detail: 'Já existem registros para o período informado no Peru',
            periodo: { de: data_de, ate: data_ate },
            existingCount,
          });
        }
      } else {
        await prisma.$disconnect();
      }

      // Consultar robo (Python) para obter dados brutos do Peru
      const data = await queryAduanetPeru(data_de, data_ate, cnpj);

      // Preparar registros e aplicar limite opcional
      const registros = Array.isArray(data?.resultados) ? (data.resultados as any[]) : [];
      const sliced = typeof limit === 'number' ? registros.slice(0, limit) : registros;
      const limited = typeof limit === 'number' && registros.length > sliced.length;
      const descricaoBase = typeof data.descricao === 'string' ? data.descricao : '';
      const descricao = limited ? (descricaoBase ? `${descricaoBase} (limitado a ${limit})` : `limitado a ${limit}`) : descricaoBase;

      // Garantir country_code nos registros para o DataTransformer
      const resultadosComPais = sliced.map(r => ({ country_code: 'PE', ...r }));

      // Persistir com resposta mínima (mesma lógica do Brasil)
      const transformer = new DataTransformer();
      await transformer.initializeBaseData();
      await transformer.processRobotResponse({
        descricao,
        total: resultadosComPais.length,
        resultados: resultadosComPais,
      });
      await transformer.disconnect();

      reply.header('Content-Type', 'application/json; charset=utf-8');
      return reply.code(200).send({
        descricao,
        total: resultadosComPais.length,
        persisted: true,
      });
    } catch (err: any) {
      request.log.error({ err }, 'Falha ao consultar robo_aduanet (Peru)');
      return reply.code(500).send({ error: 'robo_aduanet_failed', detail: err?.message });
    }
  });
};

export default peruRoutes;