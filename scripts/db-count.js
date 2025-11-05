require('dotenv/config');
const { PrismaClient } = require('@prisma/client');

(async () => {
  const prisma = new PrismaClient();
  try {
    const pc = await prisma.product.count();
    const ic = await prisma.import.count();
    console.log('product_count', pc);
    console.log('import_count', ic);
  } catch (e) {
    console.error('count_error', e);
    process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
})();