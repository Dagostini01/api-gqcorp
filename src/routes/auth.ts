import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { PrismaClient } from '@prisma/client';
import { verifyPassword } from '../utils/password';
import { AUTH_SECRET } from '../config/env';
import jwt from 'jsonwebtoken';

const authRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  // POST /auth/login -> autenticar usuário e emitir token
  app.post<{ Body: { email: string; password: string } }>(
    '/login',
    {
      schema: {
        body: {
          type: 'object',
          properties: {
            email: { type: 'string', format: 'email' },
            password: { type: 'string', minLength: 6 },
          },
          required: ['email', 'password'],
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      if (!AUTH_SECRET) {
        return reply.code(500).send({ error: 'auth_not_configured', detail: 'AUTH_SECRET não configurado' });
      }
      const prisma = new PrismaClient();
      try {
        const { email, password } = request.body;
        const user = await prisma.appUser.findUnique({ where: { email } });
        if (!user) {
          await prisma.$disconnect();
          return reply.code(401).send({ error: 'invalid_credentials' });
        }
        const ok = verifyPassword(password, user.passwordHash);
        if (!ok) {
          await prisma.$disconnect();
          return reply.code(401).send({ error: 'invalid_credentials' });
        }
        const token = jwt.sign({ sub: user.id, email: user.email }, AUTH_SECRET, { algorithm: 'HS256', expiresIn: '1h' });
        await prisma.$disconnect();
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(200).send({
          token,
          tokenType: 'Bearer',
          expiresIn: 3600,
          user: { id: user.id, email: user.email, name: user.name, phone: user.phone },
        });
      } catch (err: any) {
        await prisma.$disconnect();
        request.log.error({ err }, 'Falha no login');
        return reply.code(500).send({ error: 'login_failed', detail: err?.message });
      }
    }
  );
};

export default authRoutes;