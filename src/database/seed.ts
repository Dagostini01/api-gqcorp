import { PrismaClient } from '@prisma/client';
import { INITIAL_COUNTRIES, INITIAL_STATES_BRASIL } from './field-mapping';

async function main() {
  const prisma = new PrismaClient();
  try {
    console.log('Seed: criando países...');
    for (const countryData of INITIAL_COUNTRIES) {
      await prisma.country.upsert({
        where: { code: countryData.code },
        update: {},
        create: countryData,
      });
    }

    console.log('Seed: criando estados do Brasil...');
    const brasil = await prisma.country.findUnique({ where: { code: 'BR' } });
    if (brasil) {
      for (const stateData of INITIAL_STATES_BRASIL) {
        await prisma.state.upsert({
          where: { code_countryId: { code: stateData.code, countryId: brasil.id } },
          update: {},
          create: { ...stateData, countryId: brasil.id },
        });
      }
    }

    console.log('Seed concluído com sucesso.');
  } catch (err) {
    console.error('Erro no seed:', err);
    process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
}

main();