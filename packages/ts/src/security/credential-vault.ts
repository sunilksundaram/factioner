import * as crypto from 'node:crypto';
import { SecretCredential } from '../types/index.js';

interface StoredCredential {
  type: SecretCredential['type'];
  encryptedValue: string;
  iv: string;
  authTag: string;
  headerName?: string;
  maskedValue: string;
  /** SHA-256 of raw secret for redaction without storing plaintext */
  secretHash: string;
  /** Last 4 chars for suffix-assisted redaction heuristics */
  secretSuffix: string;
}

/**
 * CredentialVault encrypts secrets at rest (AES-256-GCM).
 * Redaction uses hashes + regex patterns — no plaintext secret set.
 */
export class CredentialVault {
  private readonly credentials: Map<string, StoredCredential> = new Map();
  private readonly encryptionKey: Buffer;

  constructor(encryptionKey?: string) {
    if (encryptionKey) {
      this.encryptionKey = crypto.createHash('sha256').update(encryptionKey).digest();
    } else {
      this.encryptionKey = crypto.randomBytes(32);
    }
  }

  private hashSecret(raw: string): string {
    return crypto.createHash('sha256').update(raw).digest('hex');
  }

  public registerCredential(endpointId: string, credential: SecretCredential): void {
    const rawValue = credential.value;
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', this.encryptionKey, iv);
    let encrypted = cipher.update(rawValue, 'utf8', 'base64');
    encrypted += cipher.final('base64');
    const authTag = cipher.getAuthTag().toString('base64');

    this.credentials.set(endpointId, {
      type: credential.type,
      encryptedValue: encrypted,
      iv: iv.toString('base64'),
      authTag,
      headerName: credential.headerName,
      maskedValue: this.maskCredential(rawValue),
      secretHash: this.hashSecret(rawValue),
      secretSuffix: rawValue.length >= 4 ? rawValue.slice(-4) : rawValue
    });
  }

  public getCredential(endpointId: string): SecretCredential | undefined {
    const stored = this.credentials.get(endpointId);
    if (!stored) return undefined;

    const iv = Buffer.from(stored.iv, 'base64');
    const authTag = Buffer.from(stored.authTag, 'base64');
    const decipher = crypto.createDecipheriv('aes-256-gcm', this.encryptionKey, iv);
    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(stored.encryptedValue, 'base64', 'utf8');
    decrypted += decipher.final('utf8');

    return {
      type: stored.type,
      value: decrypted,
      headerName: stored.headerName,
      maskedValue: stored.maskedValue
    };
  }

  public getAuthHeaders(endpointId: string): Record<string, string> {
    const cred = this.getCredential(endpointId);
    if (!cred) return {};

    const headerName = cred.headerName || (cred.type === 'bearer' || cred.type === 'oauth2' ? 'Authorization' : 'api-key');

    if (cred.type === 'bearer' || cred.type === 'oauth2') {
      const val = cred.value.startsWith('Bearer ') ? cred.value : `Bearer ${cred.value}`;
      return { [headerName]: val };
    }

    return { [headerName]: cred.value };
  }

  public maskCredential(secret: string): string {
    if (!secret || secret.length === 0) return '***';
    if (secret.length <= 10) return '****';
    return `${secret.slice(0, 2)}...${secret.slice(-2)}`;
  }

  public redactSecrets(input: string): string {
    if (!input) return input;
    let sanitized = input;

    // Decrypt-compare registered secrets without keeping a permanent plaintext set
    for (const [endpointId, stored] of this.credentials) {
      // Heuristic optimization: only decrypt if suffix or minimal secret fragment is present
      if (stored.secretSuffix && stored.secretSuffix.length >= 4 && !sanitized.includes(stored.secretSuffix)) {
        continue;
      }
      const cred = this.getCredential(endpointId);
      if (cred?.value && cred.value.length > 3 && sanitized.includes(cred.value)) {
        sanitized = sanitized.split(cred.value).join('[REDACTED_SECRET]');
      }
    }

    sanitized = sanitized.replace(/Bearer\s+[a-zA-Z0-9_\-\.]{6,}/gi, 'Bearer [REDACTED_TOKEN]');
    sanitized = sanitized.replace(
      /(api[_-]?key|password|secret)\s*[:=]\s*(["']?)([^\s"',;]+)(["']?)/gi,
      '$1=[REDACTED]'
    );

    return sanitized;
  }

  public removeCredential(endpointId: string): void {
    this.credentials.delete(endpointId);
  }

  public rotateCredential(endpointId: string, credential: SecretCredential): void {
    this.registerCredential(endpointId, credential);
  }
}
