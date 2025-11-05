import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { PrismaClient } from '@prisma/client';
import { hashPassword } from '../utils/password';

const usersRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  // POST /usuarios -> cadastrar usuário
  app.post<{ Body: { email: string; name?: string; password: string } }>(
    '/',
    {
      schema: {
        body: {
          type: 'object',
          properties: {
            email: { type: 'string', format: 'email' },
            name: { type: 'string' },
            password: { type: 'string', minLength: 6 },
          },
          required: ['email', 'password'],
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      const prisma = new PrismaClient();
      try {
        const { email, name, password } = request.body;
        const passwordHash = hashPassword(password);
        const created = await prisma.appUser.create({
          data: { email, name, passwordHash },
        });
        await prisma.$disconnect();
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(201).send({ id: created.id, email: created.email, name: created.name, createdAt: created.createdAt });
      } catch (err: any) {
        await prisma.$disconnect();
        if (err?.code === 'P2002') {
          return reply.code(409).send({ error: 'email_conflict', detail: 'Email já cadastrado' });
        }
        request.log.error({ err }, 'Falha ao criar usuário');
        return reply.code(500).send({ error: 'create_user_failed', detail: err?.message });
      }
    }
  );

  // GET /usuarios/:id -> buscar usuário
  app.get<{ Params: { id: string } }>(
    '/:id',
    {
      schema: {
        params: {
          type: 'object',
          properties: { id: { type: 'string' } },
          required: ['id'],
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      const prisma = new PrismaClient();
      try {
        const idNum = Number(request.params.id);
        if (!idNum || idNum < 1) {
          return reply.code(400).send({ error: 'invalid_id', detail: 'ID inválido' });
        }
        const user = await prisma.appUser.findUnique({ where: { id: idNum } });
        await prisma.$disconnect();
        if (!user) {
          return reply.code(404).send({ error: 'user_not_found' });
        }
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(200).send({ id: user.id, email: user.email, name: user.name, createdAt: user.createdAt, updatedAt: user.updatedAt });
      } catch (err: any) {
        await prisma.$disconnect();
        request.log.error({ err }, 'Falha ao obter usuário');
        return reply.code(500).send({ error: 'get_user_failed', detail: err?.message });
      }
    }
  );

  // POST /usuarios/:id/redefinir_senha -> atualizar senha
  app.post<{ Params: { id: string }; Body: { password: string } }>(
    '/:id/redefinir_senha',
    {
      schema: {
        params: {
          type: 'object',
          properties: { id: { type: 'string' } },
          required: ['id'],
          additionalProperties: false,
        },
        body: {
          type: 'object',
          properties: { password: { type: 'string', minLength: 6 } },
          required: ['password'],
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      const prisma = new PrismaClient();
      try {
        const idNum = Number(request.params.id);
        if (!idNum || idNum < 1) {
          return reply.code(400).send({ error: 'invalid_id', detail: 'ID inválido' });
        }
        const passwordHash = hashPassword(request.body.password);
        const updated = await prisma.appUser.update({
          where: { id: idNum },
          data: { passwordHash },
        });
        await prisma.$disconnect();
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(200).send({ id: updated.id, email: updated.email, name: updated.name, updatedAt: updated.updatedAt });
      } catch (err: any) {
        await prisma.$disconnect();
        if (err?.code === 'P2025') {
          return reply.code(404).send({ error: 'user_not_found' });
        }
        request.log.error({ err }, 'Falha ao redefinir senha');
        return reply.code(500).send({ error: 'reset_password_failed', detail: err?.message });
      }
    }
  );

  // DELETE /usuarios/:id -> remover usuário
  app.delete<{ Params: { id: string } }>(
    '/:id',
    {
      schema: {
        params: {
          type: 'object',
          properties: { id: { type: 'string' } },
          required: ['id'],
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      const prisma = new PrismaClient();
      try {
        const idNum = Number(request.params.id);
        if (!idNum || idNum < 1) {
          return reply.code(400).send({ error: 'invalid_id', detail: 'ID inválido' });
        }
        const deleted = await prisma.appUser.delete({ where: { id: idNum } });
        await prisma.$disconnect();
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(200).send({ id: deleted.id, email: deleted.email });
      } catch (err: any) {
        await prisma.$disconnect();
        if (err?.code === 'P2025') {
          return reply.code(404).send({ error: 'user_not_found' });
        }
        request.log.error({ err }, 'Falha ao deletar usuário');
        return reply.code(500).send({ error: 'delete_user_failed', detail: err?.message });
      }
    }
  );
};

export default usersRoutes;