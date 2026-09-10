import { Injectable } from '@nestjs/common';
import nodemailer, { type Transporter } from 'nodemailer';
import { env } from '../env.js';

export interface SendEmailInput {
  to: string;
  subject: string;
  text: string;
}

/**
 * Wrapper fino de `nodemailer` contra o Mailhog do `docker-compose.yml`
 * (SMTP fake — nunca envia e-mail de verdade). `secure: false` porque
 * Mailhog não faz TLS: infra de estudo local, nunca use isto em produção
 * (A02/A04 — em produção seria um provedor SMTP real com STARTTLS/SSL).
 */
@Injectable()
export class MailerService {
  private readonly transporter: Transporter = nodemailer.createTransport({
    host: env.SMTP_HOST,
    port: env.SMTP_PORT,
    secure: false,
  });

  async send(input: SendEmailInput): Promise<void> {
    await this.transporter.sendMail({
      from: env.SMTP_FROM,
      to: input.to,
      subject: input.subject,
      text: input.text,
    });
  }
}
