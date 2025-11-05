import { randomBytes, scryptSync } from 'crypto';

// Generates scrypt hash in the format: scrypt:<saltBase64>:<hashBase64>
export function hashPassword(password: string): string {
  const salt = randomBytes(16);
  const derived = scryptSync(password, salt, 64);
  return `scrypt:${salt.toString('base64')}:${derived.toString('base64')}`;
}

export function verifyPassword(password: string, stored: string): boolean {
  try {
    const [prefix, saltB64, hashB64] = stored.split(':');
    if (prefix !== 'scrypt' || !saltB64 || !hashB64) return false;
    const salt = Buffer.from(saltB64, 'base64');
    const derived = scryptSync(password, salt, 64);
    return derived.toString('base64') === hashB64;
  } catch {
    return false;
  }
}