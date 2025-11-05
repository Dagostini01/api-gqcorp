export async function sendEmail(to: string, subject: string, message: string): Promise<boolean> {
  // Stub de envio de e-mail. Em produção, integrar com SMTP/Nodemailer/SES.
  // Aqui apenas fazemos log para desenvolvimento.
  if (!to) {
    console.warn('[EMAIL] destinatário vazio, cancelando envio');
    return false;
  }
  console.log(`[EMAIL] to=${to} subject=${subject} body=${message}`);
  return true;
}