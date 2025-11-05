import { PrismaClient } from '@prisma/client';
import { INITIAL_STATES_BRASIL } from '../src/database/field-mapping';

function parseDateFromFecNumeracao(fec?: string | null): Date | undefined {
  if (!fec) return undefined;
  const s = fec.trim();
  const m = s.match(/^\s*(\d{2})\/(\d{2})\/(\d{4})\s*$/);
  if (!m) return undefined;
  const dd = parseInt(m[1], 10);
  const mm = parseInt(m[2], 10);
  const yyyy = parseInt(m[3], 10);
  if (isNaN(dd) || isNaN(mm) || isNaN(yyyy) || mm < 1 || mm > 12) return undefined;
  return new Date(yyyy, mm - 1, 1);
}

const normalize = (s: string) => s
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .toUpperCase()
  .trim();

async function resolveState(prisma: PrismaClient, input: string | null | undefined, countryId: number) {
  if (!input) return null;
  let codeCandidate = input.trim().toUpperCase();
  if (codeCandidate.length > 2) {
    const nameToCode: Record<string, string> = {};
    for (const st of INITIAL_STATES_BRASIL) {
      nameToCode[normalize(st.name)] = st.code;
    }
    const norm = normalize(codeCandidate);
    codeCandidate = nameToCode[norm] || codeCandidate;
  }
  const state = await prisma.state.findFirst({
    where: {
      countryId,
      OR: [
        { code: codeCandidate },
        { name: { equals: input, mode: 'insensitive' } }
      ]
    }
  });
  return state?.id ?? null;
}

async function main() {
  const prisma = new PrismaClient();
  try {
    console.log('Backfill: iniciando...');
    const brasil = await prisma.country.findUnique({ where: { code: 'BR' } });
    if (!brasil) {
      console.error('País BR não encontrado. Abortando.');
      process.exitCode = 1;
      return;
    }

    const rows = await prisma.import.findMany({
      where: {
        countryId: brasil.id,
        dataSource: 'COMEXSTAT',
        OR: [
          { series: null },
          { operationDate: null },
          { stateId: null },
        ],
      },
      orderBy: { id: 'desc' },
    });

    let updated = 0;
    for (const imp of rows) {
      const data: any = {};
      if (!imp.series) {
        data.series = 'BR';
      }
      if (!imp.operationDate && imp.numerationDate) {
        const d = parseDateFromFecNumeracao(imp.numerationDate);
        if (d) data.operationDate = d;
      }
      if (!imp.stateId) {
        // Tenta resolver pelo rawData (se salvo) ou deixa como está
        const raw: any = (imp as any).rawData;
        const stateName = raw?.state as string | undefined;
        if (stateName) {
          const sid = await resolveState(prisma, stateName, brasil.id);
          if (sid) data.stateId = sid;
        }
      }

      if (Object.keys(data).length > 0) {
        await prisma.import.update({ where: { id: imp.id }, data });
        updated++;
      }
    }

    console.log(`Backfill: registros atualizados = ${updated}`);
  } catch (err) {
    console.error('Erro no backfill:', err);
    process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
}

main();