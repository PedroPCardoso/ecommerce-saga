import { describe, expect, it } from 'vitest';
import { UnprocessableEventError } from '@ecommerce/contracts';
import { classifyError } from '../src/index.js';

describe('classifyError', () => {
  it('classifica UnprocessableEventError como permanente', () => {
    expect(classifyError(new UnprocessableEventError('schema inválido'))).toBe('permanent');
  });

  it('classifica erro marcado .permanent=true como permanente', () => {
    const error = new Error('regra de negócio violada') as Error & { permanent: boolean };
    error.permanent = true;
    expect(classifyError(error)).toBe('permanent');
  });

  it('classifica erro comum como retriável — fail secure: não desiste cedo demais', () => {
    expect(classifyError(new Error('ETIMEDOUT'))).toBe('retriable');
  });

  it('classifica valor não-Error como retriável', () => {
    expect(classifyError('string genérica')).toBe('retriable');
  });
});
