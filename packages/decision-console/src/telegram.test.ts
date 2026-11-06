import { describe, expect, it } from 'vitest';
import { TelegramBotApi, TelegramFetch, TelegramFailure } from './telegram';

describe('fixed-host Telegram transport', () => {
  it('sends plain text and opaque callback data through the fixed host', async () => {
    const calls: { url: string; body: unknown }[] = [];
    const fetcher: TelegramFetch = (url, init) => {
      calls.push({ url, body: JSON.parse(init.body) as unknown });
      return Promise.resolve({
        ok: true,
        status: 200,
        json: (): Promise<unknown> => Promise.resolve({ ok: true, result: { message_id: 42 } }),
      });
    };
    const api = new TelegramBotApi('123:synthetic_only', fetcher);
    expect(
      await api.send({
        chat_id: '10',
        text: '<plain text>',
        reply_markup: { inline_keyboard: [[{ text: 'Buy', callback_data: 'opaque' }]] },
      })
    ).toBe(42);
    expect(calls[0]?.url).toBe('https://api.telegram.org/bot123:synthetic_only/sendMessage');
    expect(calls[0]?.body).not.toHaveProperty('parse_mode');
    await expect(
      api.send({
        chat_id: '10',
        text: 'test',
        reply_markup: { inline_keyboard: [[{ text: 'x', callback_data: 'a'.repeat(65) }]] },
      })
    ).rejects.toThrow('64 bytes');
  });
  it('redacts transport failures and treats unknown delivery as ambiguous', async () => {
    const api = new TelegramBotApi('123:synthetic_only', () => {
      return Promise.reject(new Error('URL contained synthetic_only'));
    });
    try {
      await api.send({ chat_id: '10', text: 'test' });
      throw new Error('Expected rejection');
    } catch (error) {
      expect(error).toBeInstanceOf(TelegramFailure);
      expect((error as TelegramFailure).ambiguous).toBe(true);
      expect((error as Error).message).not.toContain('synthetic_only');
    }
  });
  it('retains retry-after for definite rate limits and bounds long polling', async () => {
    let body: unknown;
    const api = new TelegramBotApi('123:synthetic_only', (_url, init) => {
      body = JSON.parse(init.body) as unknown;
      return Promise.resolve({
        ok: false,
        status: 429,
        json: (): Promise<unknown> =>
          Promise.resolve({ ok: false, parameters: { retry_after: 5 } }),
      });
    });
    await expect(api.updates(99)).rejects.toMatchObject({ ambiguous: false, retryAfterMs: 5000 });
    expect(body).toEqual({
      offset: 99,
      limit: 25,
      timeout: 25,
      allowed_updates: ['message', 'callback_query'],
    });
  });
});
