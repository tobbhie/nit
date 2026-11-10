import { Executor } from '@nit/database-public';
import { evidenceHash } from '@nit/signals';
import { NarrativePolicy, TextExtraction, normalized, subjectKey } from './contracts';
import { InterpretedSource, NarrativeGroup, groupSources } from './scoring';

export function cosineBps(a: readonly number[], b: readonly number[]): number {
  if (a.length !== b.length || a.length === 0) return 0;
  const dot = a.reduce((n, v, i) => n + v * b[i]!, 0);
  const norm = Math.sqrt(a.reduce((n, v) => n + v * v, 0) * b.reduce((n, v) => n + v * v, 0));
  return norm === 0 ? 0 : Math.max(0, Math.min(10000, Math.round((dot / norm) * 10000)));
}
export const topicId = (
  input: InterpretedSource,
  topic: TextExtraction['topics'][number]
): string => evidenceHash({ artifact: input.artifactId, topic });
async function query<T>(db: Executor, sql: string, args: unknown[] = []): Promise<T[]> {
  const result = await db.query<T>(sql, args);
  if (result.kind === 'failure') throw result.error;
  return result.value;
}
interface Node {
  node_id: string;
  subject: NarrativeGroup['subject'];
  catalyst: string;
  vector: number[];
  bucket: Date;
  previous_node_id: string | null;
}
/** Anchors never move. New evidence cannot rewrite an earlier membership or replay. */
export async function semanticGroups(
  db: Executor,
  inputs: readonly InterpretedSource[],
  policy: NarrativePolicy,
  asOf: Date
): Promise<NarrativeGroup[]> {
  const config = policy.semantics;
  if (config === undefined) return groupSources(inputs, policy, asOf);
  const policyHash = evidenceHash({
    semantics: config,
    subjects: policy.subjects,
    window: policy.episodeWindowMs,
  });
  const groups = new Map<string, NarrativeGroup>();
  const ordered = [...inputs].sort(
    (a, b) =>
      a.source.ingestedAt.getTime() - b.source.ingestedAt.getTime() ||
      a.artifactId.localeCompare(b.artifactId)
  );
  for (const input of ordered) {
    if (input.source.ingestedAt > asOf || input.source.publishedAt > asOf)
      throw new Error('Future semantic evidence');
    for (const topic of input.extraction.topics) {
      if (topic.meaning === undefined) {
        const literal = groupSources(
          [{ ...input, extraction: { ...input.extraction, topics: [topic] } }],
          policy,
          asOf
        );
        for (const g of literal) {
          const prior = groups.get(g.key);
          if (prior === undefined) groups.set(g.key, g);
          else if (!prior.sources.some((s) => s.artifactId === input.artifactId))
            prior.sources.push(input);
        }
        continue;
      }
      const meaning = topic.meaning;
      if (
        meaning.modelVersion !== config.modelVersion ||
        meaning.vector.length !== config.dimensions
      )
        throw new Error('Semantic model/dimension mismatch');
      const memberId = topicId(input, topic);
      const start =
        Math.floor(input.source.publishedAt.getTime() / policy.episodeWindowMs) *
        policy.episodeWindowMs;
      const expires = new Date(start + policy.episodeWindowMs + policy.watchDurationMs);
      if (expires <= asOf) continue;
      let membership = (
        await query<{ node_id: string; relation: string }>(
          db,
          'SELECT node_id,relation FROM nit_semantic_members WHERE member_id=$1 AND policy_hash=$2 AND available_at<=$3',
          [memberId, policyHash, asOf]
        )
      )[0];
      if (membership === undefined) {
        if (
          (
            await query(
              db,
              'SELECT 1 FROM nit_semantic_members WHERE member_id=$1 AND policy_hash=$2',
              [memberId, policyHash]
            )
          ).length > 0
        )
          throw new Error('Cannot rewrite future semantic membership');
        const quoted = subjectKey(topic.subjectQuote, policy);
        const inferred = subjectKey(meaning.subject, policy);
        const subject = quoted.key.startsWith('literal:') ? inferred : quoted;
        // Conflicting configured identities never converge through a model label.
        const mismatch =
          !quoted.key.startsWith('literal:') &&
          !inferred.key.startsWith('literal:') &&
          quoted.key !== inferred.key;
        subject.ambiguous ||= mismatch || quoted.ambiguous || inferred.ambiguous;
        const nodes = await query<Node>(
          db,
          `SELECT node_id,subject,catalyst,vector,bucket,previous_node_id FROM nit_semantic_nodes
          WHERE policy_hash=$1 AND subject->>'key'=$2 AND available_at<=$3 AND bucket<=$4 AND bucket>=$5
          ORDER BY bucket DESC,node_id LIMIT 65`,
          [
            policyHash,
            subject.key,
            asOf,
            new Date(start),
            new Date(start - policy.temporalWindowMs),
          ]
        );
        if (nodes.length > 64) throw new Error('Semantic graph retrieval overflow');
        const eligible = nodes
          .filter((n) => n.bucket.getTime() === start)
          .map((node) => ({ node, score: cosineBps(meaning.vector, node.vector) }))
          .filter((n) => n.score >= config.mergeBps)
          .sort((a, b) => b.score - a.score || a.node.node_id.localeCompare(b.node.node_id));
        const ambiguous =
          subject.ambiguous ||
          (eligible.length > 1 &&
            eligible[0]!.score - eligible[1]!.score < config.ambiguityMarginBps);
        let nodeId = !ambiguous ? eligible[0]?.node.node_id : undefined;
        let relation =
          nodeId === undefined
            ? ambiguous
              ? 'ambiguous'
              : nodes.some((n) => n.bucket.getTime() === start)
                ? 'split'
                : 'created'
            : 'merged';
        if (nodeId === undefined) {
          nodeId = evidenceHash({ policyHash, memberId, start });
          const previous = nodes.find(
            (n) =>
              n.bucket.getTime() < start &&
              cosineBps(n.vector, meaning.vector) >= config.mergeBps &&
              !n.subject.ambiguous
          );
          if (previous !== undefined && !ambiguous) relation = 'revival';
          await query(
            db,
            `INSERT INTO nit_semantic_nodes(node_id,policy_hash,subject,catalyst,vector,bucket,previous_node_id,available_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`,
            [
              nodeId,
              policyHash,
              JSON.stringify({ ...subject, ambiguous }),
              meaning.event,
              JSON.stringify(meaning.vector),
              new Date(start),
              !ambiguous ? (previous?.node_id ?? null) : null,
              asOf,
            ]
          );
        }
        await query(
          db,
          `INSERT INTO nit_semantic_members(member_id,policy_hash,node_id,relation,compared,available_at)
          VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
          [
            memberId,
            policyHash,
            nodeId,
            relation,
            JSON.stringify(
              nodes.map((n) => ({ nodeId: n.node_id, score: cosineBps(meaning.vector, n.vector) }))
            ),
            asOf,
          ]
        );
        membership = { node_id: nodeId, relation };
      }
      const node = (
        await query<Node>(
          db,
          'SELECT node_id,subject,catalyst,vector,bucket,previous_node_id FROM nit_semantic_nodes WHERE node_id=$1',
          [membership.node_id]
        )
      )[0]!;
      let group = groups.get(node.node_id);
      if (group === undefined) {
        group = {
          key: node.node_id,
          subject: node.subject,
          category: topic.category,
          catalystKey: normalized(node.catalyst),
          startsAt: node.bucket,
          expiresAt: expires,
          sources: [],
          memberIds: [],
          previousGroupKey: node.previous_node_id,
        };
        groups.set(group.key, group);
      }
      group.memberIds!.push(memberId);
      group.subject.aliases = [
        ...new Set([...group.subject.aliases, topic.subjectQuote, meaning.subject]),
      ].sort();
      if (group.category !== topic.category) group.category = 'mixed';
      if (!group.sources.some((s) => s.artifactId === input.artifactId)) group.sources.push(input);
    }
  }
  return [...groups.values()].sort((a, b) => a.key.localeCompare(b.key));
}
