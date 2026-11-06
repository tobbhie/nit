import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { contentHash, evidenceHash } from '@nit/signals';
import { GuideBundle } from './types';
const GUIDE_FILES = ['SOUL.md', 'INVESTIGATION.md', 'OUTPUT.md'] as const;
export async function loadAgentGuides(): Promise<GuideBundle> {
  const files: Record<string, { hash: string; text: string }> = {};
  for (const name of GUIDE_FILES) {
    const text = await readFile(join(__dirname, '../guides', name), 'utf8');
    if (Buffer.byteLength(text) > 12000) throw new Error('GUIDE_TOO_LARGE');
    files[name] = { hash: contentHash(text), text };
  }
  return { version: 1, hash: evidenceHash(files), files };
}
