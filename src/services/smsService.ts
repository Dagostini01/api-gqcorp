import { TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM, NODE_ENV } from '../config/env';
import twilio from 'twilio';

export async function sendSms(phone: string, message: string): Promise<boolean> {
  // Se Twilio estiver configurado, tenta enviar SMS real
  if (TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN && TWILIO_FROM) {
    try {
      const client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
      const res = await client.messages.create({ from: TWILIO_FROM, to: phone, body: message });
      console.log(`[SMS] Twilio enviado: sid=${res.sid} to=${phone}`);
      return true;
    } catch (err: any) {
      console.error('[SMS] Erro ao enviar via Twilio', err?.message || err);
      return false;
    }
  }

  // Fallback de desenvolvimento (stub)
  console.warn('[SMS] Twilio não configurado. Usando stub de desenvolvimento.');
  console.log(`[SMS] to=${phone} msg=${message}`);
  // Em desenvolvimento, considerar sucesso para permitir fluxo de testes
  return NODE_ENV !== 'production';
}