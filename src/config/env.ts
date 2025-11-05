export const PORT: number = Number(process.env.PORT || 3000);
export const PYTHON_BIN: string = process.env.PYTHON_BIN || 'python';
export const COMEX_INSECURE: string | undefined = process.env.COMEX_INSECURE;
export const COMEX_CA_BUNDLE: string | undefined = process.env.COMEX_CA_BUNDLE;
export const BOT_DEBUG: string | undefined = process.env.BOT_DEBUG;
export const SAVE_RAW_DATA: boolean = String(process.env.SAVE_RAW_DATA || '').toLowerCase() === 'true';
export const NODE_ENV: string | undefined = process.env.NODE_ENV;
export const AUTH_SECRET: string | undefined = process.env.AUTH_SECRET;
// SMS (Twilio)
export const TWILIO_ACCOUNT_SID: string | undefined = process.env.TWILIO_ACCOUNT_SID;
export const TWILIO_AUTH_TOKEN: string | undefined = process.env.TWILIO_AUTH_TOKEN;
export const TWILIO_FROM: string | undefined = process.env.TWILIO_FROM;