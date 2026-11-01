/**
 * Version metadata for auditability
 */
export interface Version {
  schemaVersion: string;
  buildId: string;
  commitHash?: string;
  deployedAt?: Date;
}

export interface Versioned {
  version: Version;
}

export function createVersion(schemaVersion: string, buildId: string): Version {
  const version: Version = {
    schemaVersion,
    buildId,
  };

  const commitHash = process.env['GIT_COMMIT'];
  if (commitHash !== undefined) {
    version.commitHash = commitHash;
  }

  const deployedAt = process.env['DEPLOYED_AT'];
  if (deployedAt !== undefined) {
    version.deployedAt = new Date(deployedAt);
  }

  return version;
}
