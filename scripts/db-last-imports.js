require('dotenv/config');
const { PrismaClient } = require('@prisma/client');

(async () => {
  const prisma = new PrismaClient();
  try {
    const rows = await prisma.import.findMany({
      select: {
        id: true,
        declarationNumber: true,
        fobUsd: true,
        cifUsd: true,
        netWeight: true,
        dataSource: true,
        rawData: true,
      },
      orderBy: { id: 'desc' },
      take: 5,
    });
    console.log(JSON.stringify(rows, null, 2));
  } catch (e) {
    console.error('query_error', e);
    process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
})();