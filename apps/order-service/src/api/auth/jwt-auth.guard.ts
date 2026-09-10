import { type CanActivate, type ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import jwt from 'jsonwebtoken';
import type { Request } from 'express';
import { z } from 'zod';
import { env } from '../../env.js';

const claimsSchema = z.object({ sub: z.string().uuid() });

export interface AuthenticatedRequest extends Request {
  customerId: string;
}

/**
 * JWT HS256 — suficiente para dev/estudo. Em produção seria RS256 via IdP
 * externo com rotação de chave (dívida documentada, fora de escopo aqui).
 * `customerId` é extraído do claim `sub`, nunca aceito de outro lugar —
 * é o que impede um cliente de ler/criar pedido em nome de outro (A01).
 */
@Injectable()
export class JwtAuthGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const header = request.headers.authorization;
    if (!header?.startsWith('Bearer ')) {
      throw new UnauthorizedException('Authorization header ausente ou malformado');
    }

    const token = header.slice('Bearer '.length);
    let payload: unknown;
    try {
      payload = jwt.verify(token, env.JWT_SECRET, { issuer: env.JWT_ISSUER, algorithms: ['HS256'] });
    } catch {
      throw new UnauthorizedException('Token inválido ou expirado');
    }

    const claims = claimsSchema.safeParse(payload);
    if (!claims.success) {
      throw new UnauthorizedException('Token sem claim "sub" válida');
    }

    request.customerId = claims.data.sub;
    return true;
  }
}
