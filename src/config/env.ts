export const PORT: number = Number(process.env.PORT || 3000);
export const PYTHON_BIN: string = process.env.PYTHON_BIN || 'python';
export const COMEX_INSECURE: string | undefined = process.env.COMEX_INSECURE;
export const COMEX_CA_BUNDLE: string | undefined = process.env.COMEX_CA_BUNDLE;
export const BOT_DEBUG: string | undefined = process.env.BOT_DEBUG;