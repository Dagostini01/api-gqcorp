import { spawn } from 'child_process';

export interface RunResult {
  stdout: string;
  stderr: string;
  code: number | null;
}

export function runProcess(
  command: string,
  args: string[],
  opts?: { cwd?: string; env?: NodeJS.ProcessEnv }
): Promise<RunResult> {
  return new Promise((resolve, reject) => {
    try {
      const proc = spawn(command, args, {
        shell: false,
        cwd: opts?.cwd,
        env: { ...process.env, ...opts?.env },
      });

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (chunk) => {
        stdout += chunk.toString();
      });
      proc.stderr.on('data', (chunk) => {
        stderr += chunk.toString();
      });

      proc.on('error', (err) => {
        reject(err);
      });

      proc.on('close', (code) => {
        resolve({ stdout, stderr, code });
      });
    } catch (err) {
      reject(err);
    }
  });
}