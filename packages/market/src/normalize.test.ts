import { describe, expect, it } from 'vitest';
import { Chain, LaunchSource, ValidationError } from '@nit/domain';
import { DISCOVERED, LAUNCHED, LaunchOverrides, launch } from './in-memory.fixtures';
import { NormalizedTokenLaunch, normalizeSourceLaunch } from './normalize';

/**
 * Normalize the fixture launch and fail the test if it was rejected.
 *
 * Keeps the happy-path assertions from restating the same narrowing four times.
 */
function normalized(
  overrides: LaunchOverrides = {},
  chain: Chain = Chain.SOLANA,
  source: LaunchSource = LaunchSource.PUMP_FUN
): NormalizedTokenLaunch {
  const result = normalizeSourceLaunch(chain, source, launch(overrides), DISCOVERED);
  if (result.kind === 'failure') {
    throw result.error;
  }
  return result.value;
}

/** The rejection reason, or a thrown failure if the launch was accepted. */
function rejection(
  overrides: LaunchOverrides = {},
  chain: Chain = Chain.SOLANA,
  source: LaunchSource = LaunchSource.PUMP_FUN
): ValidationError {
  const result = normalizeSourceLaunch(chain, source, launch(overrides), DISCOVERED);
  if (result.kind === 'success') {
    throw new Error('Expected the launch to be rejected');
  }
  return result.error;
}

describe('normalizeSourceLaunch', () => {
  it('carries the launchpad fields onto the canonical token', () => {
    const token = normalized({
      address: 'Mint1111111111111111111111111111111111111Bb',
      ticker: 'PEPE',
      name: 'Pepe Coin',
      creatorWallet: 'Creator111111111111111111111111111111111Cc',
      description: 'a frog',
      artworkUrl: 'https://cdn.invalid/pepe.png',
    });

    expect(token.chain).toBe(Chain.SOLANA);
    expect(token.launchSource).toBe(LaunchSource.PUMP_FUN);
    expect(token.address).toBe('Mint1111111111111111111111111111111111111Bb');
    expect(token.ticker).toBe('PEPE');
    expect(token.name).toBe('Pepe Coin');
    expect(token.creatorWallet).toBe('Creator111111111111111111111111111111111Cc');
    expect(token.description).toBe('a frog');
    expect(token.artworkUrl).toBe('https://cdn.invalid/pepe.png');
    expect(token.launchTime).toEqual(LAUNCHED);
    expect(token.discoveredAt).toEqual(DISCOVERED);
  });

  it('assigns an identifier the launchpad did not supply', () => {
    // Token ids are ours. A launchpad's own id would make identity depend on a
    // third party's numbering, and two launchpads would eventually collide.
    const first = normalized({ address: 'Aaa1' });
    const second = normalized({ address: 'Bbb2' });

    expect(first.tokenId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-/);
    expect(second.tokenId).not.toBe(first.tokenId);
  });

  it('keeps the launchpad payload verbatim for a later worker to re-read', () => {
    const token = normalized({ address: 'Raw1' });

    expect(token.rawPayload).toEqual({ mint: 'Raw1', source: 'fixture' });
  });

  it('preserves the case of a mint address', () => {
    // Base58 is case-sensitive: case-folding an address produces one that does
    // not exist on chain while looking entirely plausible.
    const mixed = 'So11111111111111111111111111111111111111Aa';
    const token = normalized({ address: mixed });

    expect(token.address).toBe(mixed);
  });

  it('preserves the case of a ticker and a name', () => {
    // Case-insensitivity belongs in the query, through the LOWER(...) indexes,
    // not in the stored value — the ticker is displayed to a human.
    const token = normalized({ ticker: 'wIf', name: 'dogWifHat' });

    expect(token.ticker).toBe('wIf');
    expect(token.name).toBe('dogWifHat');
  });

  it('trims surrounding whitespace from the fields it keeps', () => {
    const token = normalized({
      address: '  Trim1  ',
      ticker: '  TRIM  ',
      name: '  Trim Token  ',
      creatorWallet: '  Wallet1  ',
    });

    expect(token.address).toBe('Trim1');
    expect(token.ticker).toBe('TRIM');
    expect(token.name).toBe('Trim Token');
    expect(token.creatorWallet).toBe('Wallet1');
  });

  it.each([
    ['address', { address: '   ' }],
    ['ticker', { ticker: '' }],
    ['name', { name: '  ' }],
  ] as const)('rejects a launch with no %s', (field, overrides) => {
    const error = rejection(overrides);

    expect(error).toBeInstanceOf(ValidationError);
    expect(error.message).toContain(field);
  });

  it('rejects an unparseable launch time', () => {
    // `new Date('nonsense')` is a Date, so the type system is satisfied and only
    // a value check catches it. Persisting it would store NULL-like garbage in a
    // NOT NULL column, or worse, succeed.
    const error = rejection({ launchTime: new Date('not a date') });

    expect(error).toBeInstanceOf(ValidationError);
    expect(error.message).toContain('launchTime');
  });

  it('rejects a chain it does not recognise', () => {
    // The parameter is typed as an enum, but an adapter's output crosses a JSON
    // boundary through a job payload, so the type is a claim rather than a
    // guarantee — and an unknown chain forks token identity instead of failing.
    const error = rejection({}, 'ethereum' as Chain);

    expect(error.message).toContain('chain');
  });

  it('rejects a launch source it does not recognise', () => {
    const error = rejection({}, Chain.SOLANA, 'uniswap' as LaunchSource);

    expect(error.message).toContain('launch source');
  });

  it.each([
    ['creatorWallet', { creatorWallet: '   ' }],
    ['description', { description: '  ' }],
    ['artworkUrl', { artworkUrl: '' }],
  ] as const)('treats a blank %s as absent rather than empty', (field, overrides) => {
    // The enrichment workers branch on absence: an artwork URL of '' would
    // enqueue a download of nothing.
    const token = normalized(overrides);

    expect(token[field]).toBeUndefined();
    expect(field in token).toBe(false);
  });

  it('omits optional fields the launchpad did not send at all', () => {
    const token = normalized();

    expect('creatorWallet' in token).toBe(false);
    expect('description' in token).toBe(false);
    expect('artworkUrl' in token).toBe(false);
  });

  it('reports the address alongside a rejected launch time', () => {
    // A rejection has to be actionable: the operator needs to know which mint in
    // the window was malformed, and the address is the only handle that exists
    // before an identifier is assigned.
    const error = rejection({ address: 'Bad1', launchTime: new Date('nope') });

    expect(error.details).toMatchObject({ address: 'Bad1' });
  });
});
