import path from 'path';
import { runProcess } from '../utils/runProcess';
import { PYTHON_BIN, BOT_DEBUG } from '../config/env';

export async function queryAduanetPeru(dataDe: string, dataAte: string, cnpj: string): Promise<any> {
  const scriptPath = path.resolve(process.cwd(), 'src', 'bot', 'peru', 'robo_aduanet.py');

  const args = [scriptPath];
  if (BOT_DEBUG === '1') args.push('--debug');
  args.push(String(dataDe), String(dataAte), 'importacao', String(cnpj));

  const res = await runProcess(PYTHON_BIN, args, {
    env: {
      PYTHONIOENCODING: 'utf-8',
      PYTHONUTF8: '1',
    },
  });

  if (res.code !== 0) {
    throw new Error(`python_process_error: code=${res.code}; stderr=${res.stderr}`);
  }

  const trimmed = res.stdout.trim();
  try {
    return JSON.parse(trimmed);
  } catch (e: any) {
    throw new Error(`invalid_json_from_bot: ${e.message}; raw=${trimmed.slice(0, 500)}`);
  }
}