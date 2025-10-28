import path from 'path';
import { runProcess } from '../utils/runProcess';
import { PYTHON_BIN, COMEX_INSECURE, COMEX_CA_BUNDLE, BOT_DEBUG } from '../config/env';

export async function queryRoboComex(ncm: string, dataDe: string, dataAte: string): Promise<any> {
  const scriptPath = path.resolve(process.cwd(), 'src', 'bot', 'brasil', 'robo_comex.py');

  const args = [scriptPath];
  if (BOT_DEBUG === '1') args.push('--debug');
  args.push(String(ncm), String(dataDe), String(dataAte));

  const res = await runProcess(PYTHON_BIN, args, {
    env: {
      COMEX_INSECURE: COMEX_INSECURE ?? '1',
      COMEX_CA_BUNDLE: COMEX_CA_BUNDLE,
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