import { PrismaClient } from '@prisma/client';

async function main() {
  const prisma = new PrismaClient();
  try {
    const countries = await prisma.country.findMany({ select: { code: true, name: true } });
    console.log('Countries count:', countries.length);
    console.log('Countries sample:', countries.slice(0, 5));

    const br = await prisma.country.findUnique({ where: { code: 'BR' } });
    if (br) {
      const statesCount = await prisma.state.count({ where: { countryId: br.id } });
      console.log('BR states count:', statesCount);
    }
  } catch (err) {
    console.error('Erro ao consultar o banco via Prisma:', err);
    process.exitCode = 1;
  }
}

main();