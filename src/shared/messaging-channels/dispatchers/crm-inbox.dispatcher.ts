import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EvoExtensionPoints } from '../../../evo-extension-points/registry';
import {
  ChannelDispatchInput,
  DispatchResult,
  IChannelDispatcher,
} from '../interfaces/channel-dispatcher.interface';

interface CrmMessagePayload {
  source_id: string;
  inbox_id: string;
  contact_id: string;
  status?: string;
  message: {
    content: string;
    message_type?: string;
    private?: boolean;
    content_attributes?: Record<string, unknown>;
    template_params?: {
      name: string;
      category?: string;
      language?: string;
      processed_params?: Record<string, unknown>;
    };
  };
}

/**
 * Delivers a campaign message by creating a conversation in the EvoAI CRM inbox
 * (`POST /api/v1/conversations`). Extracted verbatim from
 * CampaignMessageSenderService (story 2.2 / EVO-1202) — channel-agnostic: the
 * channel is carried by `inboxId`, not by the transport.
 */
@Injectable()
export class CrmInboxDispatcher implements IChannelDispatcher {
  private readonly logger = new Logger(CrmInboxDispatcher.name);
  private readonly baseURL: string;
  private readonly serviceToken: string;
  private readonly timeout: number = 30000;

  constructor(private readonly configService: ConfigService) {
    this.baseURL =
      this.configService.get<string>('EVOAI_CRM_BASE_URL') ||
      'http://localhost:3000';
    this.serviceToken =
      this.configService.get<string>('EVOAI_CRM_API_TOKEN') || '';

    if (!this.serviceToken) {
      this.logger.warn('EVOAI_CRM_API_TOKEN not configured');
    }
  }

  async dispatch(input: ChannelDispatchInput): Promise<DispatchResult> {
    const url = `${this.baseURL}/api/v1/conversations`;

    const payload: CrmMessagePayload = {
      source_id: `campaign_${input.campaignId}_${Date.now()}`,
      inbox_id: input.inboxId,
      contact_id: input.contactId,
      status: 'open',
      message: {
        content: input.content,
        message_type: 'outgoing',
        private: false,
        content_attributes: {
          campaign_id: input.campaignId,
          sent_at: new Date().toISOString(),
        },
        ...(input.templateParams && {
          template_params: input.templateParams,
        }),
      },
    };

    const start = Date.now();
    try {
      const response = await this.executeRequest(
        url,
        {
          method: 'POST',
          headers: this.getHeaders(),
          body: JSON.stringify(payload),
        },
        input.transportRetries,
      );
      const latencyMs = Date.now() - start;

      if (!response.ok) {
        const errorText = await response.text();
        this.logger.error('CRM API error', {
          status: response.status,
          error: errorText,
        });

        return {
          success: false,
          // Preserve the legacy error string verbatim (behavior-preserving
          // extraction): callers map error.message into SendMessageResult.error.
          error: {
            code: String(response.status),
            message: `CRM API error: ${response.status} - ${errorText}`,
          },
          statusCode: response.status,
          latencyMs,
        };
      }

      const data = (await response.json()) as {
        id?: string;
        messages?: Array<{ id?: string }>;
      };

      return {
        success: true,
        conversationId: data.id,
        messageId: data.messages?.[0]?.id,
        statusCode: response.status,
        latencyMs,
      };
    } catch (error) {
      const latencyMs = Date.now() - start;
      this.logger.error('Failed to send message to CRM', {
        error: (error as Error).message,
      });

      return {
        success: false,
        error: { code: 'DISPATCH_ERROR', message: (error as Error).message },
        latencyMs,
      };
    }
  }

  private async executeRequest(
    url: string,
    options: RequestInit,
    maxRetries: number = 3,
  ): Promise<Response> {
    let lastError: Error = new Error('Unknown error');

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        let response: Response;
        try {
          response = await fetch(url, {
            ...options,
            signal: controller.signal,
          });
        } finally {
          // Also on a rejected fetch — otherwise the abort timer leaks for
          // the full 30s after every network error.
          clearTimeout(timeoutId);
        }

        if (response.status === 429 && attempt < maxRetries) {
          const retryAfter = response.headers.get('Retry-After');
          const waitTime = retryAfter ? parseInt(retryAfter) * 1000 : 5000;

          this.logger.warn(
            `Rate limited by CRM API, retrying in ${waitTime}ms`,
            { attempt, maxRetries },
          );

          await new Promise((resolve) => setTimeout(resolve, waitTime));
          continue;
        }

        return response;
      } catch (error) {
        lastError = error as Error;

        this.logger.warn(`Request failed [Attempt ${attempt}/${maxRetries}]`, {
          error: (error as Error).message,
        });

        if (attempt < maxRetries) {
          const waitTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          await new Promise((resolve) => setTimeout(resolve, waitTime));
        }
      }
    }

    throw lastError;
  }

  private getHeaders(): Record<string, string> {
    return {
      ...EvoExtensionPoints.get('outbound_headers')(),
      'Content-Type': 'application/json',
      'X-Service-Token': this.serviceToken,
      'User-Agent': 'EvoAI-Campaign/1.0',
    };
  }
}
