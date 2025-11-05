import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { PrismaClient } from '@prisma/client';
import { hashPassword } from '../utils/password';
import { generateOtpCode, hashOtp, verifyOtp } from '../utils/otp';
import { sendSms } from '../services/smsService';
import { NODE_ENV } from '../config/env';

const usersRoutes: FastifyPluginAsync = async (app: FastifyInstance) => {
  // POST /usuarios -> cadastrar usuário
  app.post<{ Body: { email: string; name?: string; phone: string; password: string } }>(
    '/',
    {
      schema: {
        body: {
          type: 'object',
          properties: {
            email: { type: 'string', format: 'email' },
            name: { type: 'string' },
            phone: { type: 'string', pattern: '^\\+?[0-9]{8,20}$' },
            password: { type: 'string', minLength: 6 },
          },
          required: ['email', 'password', 'phone'],
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      const prisma = new PrismaClient();
      try {
        const { email, name, phone, password } = request.body;
        const passwordHash = hashPassword(password);
        const created = await prisma.appUser.create({
          data: { email, name, phone, passwordHash },
        });
        await prisma.$disconnect();
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(201).send({ id: created.id, email: created.email, name: created.name, phone: created.phone, createdAt: created.createdAt });
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

  // GET /usuarios/lookup?phone=... -> buscar usuário por telefone
  app.get<{ Querystring: { phone: string } }>(
    '/lookup',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            phone: { type: 'string', pattern: '^\\+?[0-9]{8,20}$' },
          },
          required: ['phone'],
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      const prisma = new PrismaClient();
      try {
        const { phone } = request.query;
        const user = await prisma.appUser.findFirst({ where: { phone } });
        await prisma.$disconnect();
        if (!user) {
          return reply.code(404).send({ error: 'user_not_found' });
        }
        reply.header('Content-Type', 'application/json; charset=utf-8');
        return reply.code(200).send({ id: user.id, email: user.email, name: user.name, phone: user.phone });
      } catch (err: any) {
        await prisma.$disconnect();
        request.log.error({ err }, 'Falha ao buscar usuário por telefone');
        return reply.code(500).send({ error: 'lookup_failed', detail: err?.message });
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
        return reply.code(200).send({ id: user.id, email: user.email, name: user.name, phone: user.phone, createdAt: user.createdAt, updatedAt: user.updatedAt });
      } catch (err: any) {
        await prisma.$disconnect();
        request.log.error({ err }, 'Falha ao obter usuário');
        return reply.code(500).send({ error: 'get_user_failed', detail: err?.message });
      }
    }
  );

  

  // POST /usuarios/:id/redefinir_senha/otp -> enviar código por SMS
  app.post<{ Params: { id: string } }>(
    '/:id/redefinir_senha/otp',
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
        if (!user) {
          await prisma.$disconnect();
          return reply.code(404).send({ error: 'user_not_found' });
        }
        if (!user.phone) {
          await prisma.$disconnect();
          return reply.code(400).send({ error: 'phone_missing', detail: 'Usuário não possui telefone cadastrado' });
        }
        // Apagar OTPs antigos e criar novo
        await prisma.passwordResetOTP.deleteMany({ where: { userId: idNum } });
        const code = generateOtpCode(6);
        const codeHash = hashOtp(code);
        const expires = new Date(Date.now() + 10 * 60 * 1000); // 10min
        await prisma.passwordResetOTP.create({ data: { userId: idNum, codeHash, expiresAt: expires } });
        const message = `Seu código de redefinição: ${code}`;
        await sendSms(user.phone!, message);
        await prisma.$disconnect();
        reply.header('Content-Type', 'application/json; charset=utf-8');
        const payload: any = { status: 'sent', expiresAt: expires.toISOString() };
        if (NODE_ENV !== 'production') payload.code = code; // apenas para desenvolvimento/testes
        return reply.code(200).send(payload);
      } catch (err: any) {
        await prisma.$disconnect();
        request.log.error({ err }, 'Falha ao enviar OTP por SMS');
        return reply.code(500).send({ error: 'send_otp_failed', detail: err?.message });
      }
    }
  );

  // POST /usuarios/:id/redefinir_senha -> atualizar senha
  app.post<{ Params: { id: string }; Body: { code: string; password: string } }>(
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
          properties: { code: { type: 'string', minLength: 4, maxLength: 8 }, password: { type: 'string', minLength: 6 } },
          required: ['code', 'password'],
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
        // Buscar OTP válido
        const otp = await prisma.passwordResetOTP.findFirst({ where: { userId: idNum }, orderBy: { id: 'desc' } });
        if (!otp) {
          await prisma.$disconnect();
          return reply.code(400).send({ error: 'otp_required', detail: 'Solicite o código por SMS antes de redefinir' });
        }
        if (new Date(otp.expiresAt).getTime() < Date.now()) {
          await prisma.passwordResetOTP.delete({ where: { id: otp.id } });
          await prisma.$disconnect();
          return reply.code(400).send({ error: 'otp_expired', detail: 'Código expirado, solicite novamente' });
        }
        if (otp.attempts >= 5) {
          await prisma.$disconnect();
          return reply.code(429).send({ error: 'too_many_attempts', detail: 'Muitas tentativas inválidas, solicite novo código' });
        }
        const ok = verifyOtp(request.body.code, otp.codeHash);
        if (!ok) {
          await prisma.passwordResetOTP.update({ where: { id: otp.id }, data: { attempts: { increment: 1 } } });
          await prisma.$disconnect();
          return reply.code(400).send({ error: 'invalid_code', detail: 'Código inválido' });
        }
        const passwordHash = hashPassword(request.body.password);
        const updated = await prisma.appUser.update({ where: { id: idNum }, data: { passwordHash } });
        // Consumir OTP
        await prisma.passwordResetOTP.deleteMany({ where: { userId: idNum } });
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