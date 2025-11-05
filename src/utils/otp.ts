import { randomBytes, scryptSync, timingSafeEqual } from 'crypto';

export function generateOtpCode(length = 6): string {
  const digits = '0123456789';
  let out = '';
  for (let i = 0; i < length; i++) out += digits[Math.floor(Math.random() * digits.length)];
  return out;
}

export function hashOtp(code: string): string {
  const salt = randomBytes(16);
  const hash = scryptSync(code, salt, 32);
  return `scrypt:${salt.toString('base64')}:${hash.toString('base64')}`;
}

export function verifyOtp(code: string, stored: string): boolean {
  const parts = stored.split(':');
  if (parts.length !== 3 || parts[0] !== 'scrypt') return false;
  const salt = Buffer.from(parts[1], 'base64');
  const expected = Buffer.from(parts[2], 'base64');
  const actual = scryptSync(code, salt, 32);
  return timingSafeEqual(actual, expected);
}