/**
 * Unit tests for CrmClientService.
 *
 * Mocks `global.fetch` (the existing service uses native fetch, not axios).
 * Resets the static cache + circuit breaker before each test so they don't
 * leak across cases.
 */
import {
  BadRequestException,
  NotFoundException,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';

// Silence @temporalio/activity log calls under unit-test (no activity context).
jest.mock('@temporalio/activity', () => ({
  log: {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  },
}));

process.env.EVOAI_CRM_BASE_URL = 'http://crm-test.local';
process.env.EVOAI_CRM_API_TOKEN = 'svc-token';
process.env.EVOAI_CRM_RETRY_MAX_ATTEMPTS = '2';
process.env.EVOAI_CRM_CIRCUIT_THRESHOLD = '3';
// Generic-path hardening (EVO-1205): single zero-delay retry keeps these
// status-mapping cases fast. The (1s, 2s, 4s) schedule + the dedicated
// ContactsClientUnavailableException contract are covered in
// crm-client.hardening.spec.ts.
process.env.EVOAI_CRM_CLIENT_RETRY_BACKOFF_MS = '0';

import { CrmClientService } from './crm-client.service';
import { EvoExtensionPoints } from '../../evo-extension-points';

function buildFetchResponse(opts: {
  ok?: boolean;
  status: number;
  body?: any;
  headers?: Record<string, string>;
}): any {
  const headers = opts.headers ?? {};
  return {
    ok: opts.ok ?? (opts.status >= 200 && opts.status < 300),
    status: opts.status,
    statusText: `HTTP ${opts.status}`,
    headers: {
      get: (key: string) => headers[key] ?? null,
    },
    json: async () => opts.body,
    text: async () =>
      typeof opts.body === 'string' ? opts.body : JSON.stringify(opts.body),
  };
}

describe('CrmClientService', () => {
  let service: CrmClientService;
  let fetchMock: jest.Mock;

  beforeEach(() => {
    CrmClientService.clearCacheForTests();
    CrmClientService.resetCircuitBreakerForTests();
    fetchMock = jest.fn();
    (global as any).fetch = fetchMock;
    service = new CrmClientService();
  });

  describe('generic GET — caching', () => {
    it('caches GET on success and returns cached value on second call', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({ status: 200, body: { id: 'abc' } }),
      );

      const a = await service.get<any>('/api/v1/contacts/abc');
      const b = await service.get<any>('/api/v1/contacts/abc');

      expect(a).toEqual({ id: 'abc' });
      expect(b).toEqual({ id: 'abc' });
      expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('bypasses cache when noCache=true', async () => {
      fetchMock
        .mockResolvedValueOnce(
          buildFetchResponse({ status: 200, body: { id: 'a' } }),
        )
        .mockResolvedValueOnce(
          buildFetchResponse({ status: 200, body: { id: 'a-v2' } }),
        );

      await service.get<any>('/api/v1/contacts/a');
      const fresh = await service.get<any>('/api/v1/contacts/a', {
        noCache: true,
      });

      expect(fresh).toEqual({ id: 'a-v2' });
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });
  });

  describe('status mapping', () => {
    it('returns null on 404 for GET', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({ status: 404, body: { error: 'not found' } }),
      );

      const result = await service.get<any>('/api/v1/contacts/missing');
      expect(result).toBeNull();
    });

    it('throws NotFoundException on 404 for PATCH (write)', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({ status: 404, body: {} }),
      );

      await expect(
        service.patch('/api/v1/contacts/missing', { foo: 'bar' }),
      ).rejects.toBeInstanceOf(NotFoundException);
    });

    it('throws UnauthorizedException on 401', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({ status: 401, body: {} }),
      );

      await expect(service.get('/api/v1/contacts/x')).rejects.toBeInstanceOf(
        UnauthorizedException,
      );
    });

    it('throws BadRequestException on 422 with body', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({
          status: 422,
          body: { errors: ['invalid email'] },
        }),
      );

      await expect(
        service.post('/api/v1/contacts/x/labels', { labels: ['vip'] }),
      ).rejects.toBeInstanceOf(BadRequestException);
    });

    // EVO-2203: a refusal on the generic path must read as its reason, not as a
    // generic "Bad Request Exception". The journey nodes go through executeRequest
    // instead — covered under "pipeline node path" below.
    it('surfaces the CRM error message on a 422 envelope, keeping the code', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({
          status: 422,
          body: {
            success: false,
            error: {
              code: 'PIPELINE_ARCHIVED',
              message: 'Pipeline is archived and cannot receive conversations',
            },
          },
        }),
      );

      await expect(
        service.post('/api/v1/pipelines/p1/pipeline_items', {
          type: 'conversation',
        }),
      ).rejects.toMatchObject({
        message: 'Pipeline is archived and cannot receive conversations',
        response: { error: { code: 'PIPELINE_ARCHIVED' } },
      });
    });

    // Controllers that answer outside the envelope helper put the reason in a
    // top-level `message` (render_record_invalid's fallback). Lifting the envelope
    // reason must not overwrite it with the placeholder.
    it('keeps a top-level message on a 422 body with no error envelope', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({
          status: 422,
          body: { message: 'Email is invalid', attributes: ['email'] },
        }),
      );

      await expect(service.post('/api/v1/contacts', {})).rejects.toMatchObject({
        message: 'Email is invalid',
      });
    });

    it('throws ServiceUnavailableException on 5xx after exhausting retries', async () => {
      fetchMock.mockResolvedValue(
        buildFetchResponse({ status: 500, body: { error: 'boom' } }),
      );

      await expect(service.get('/api/v1/contacts/x')).rejects.toBeInstanceOf(
        ServiceUnavailableException,
      );
      // 2 attempts (retryMaxAttempts=2)
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });
  });

  describe('circuit breaker', () => {
    it('opens after consecutive failures and short-circuits next call', async () => {
      fetchMock.mockResolvedValue(
        buildFetchResponse({ status: 500, body: {} }),
      );

      // 3 calls, each retries twice -> hits threshold of 3 consecutive failures
      // (CircuitBreaker.consecutiveFailures bumps per execute() failure, not per fetch).
      for (let i = 0; i < 3; i++) {
        await expect(
          service.get(`/api/v1/contacts/${i}`),
        ).rejects.toBeInstanceOf(ServiceUnavailableException);
      }

      const callsBefore = fetchMock.mock.calls.length;

      // Fourth call: circuit should be open — no fetch made.
      await expect(service.get('/api/v1/contacts/4')).rejects.toBeInstanceOf(
        ServiceUnavailableException,
      );

      expect(fetchMock.mock.calls.length).toBe(callsBefore);
    });

    it('does NOT trip circuit on terminal 4xx (server is healthy)', async () => {
      fetchMock.mockResolvedValue(
        buildFetchResponse({ status: 401, body: {} }),
      );

      // 5 consecutive 401s — circuit must stay closed.
      for (let i = 0; i < 5; i++) {
        await expect(service.get('/api/v1/contacts/x')).rejects.toBeInstanceOf(
          UnauthorizedException,
        );
      }
    });
  });

  describe('Retry-After honored on 429', () => {
    it('waits Retry-After seconds before retrying', async () => {
      jest.useFakeTimers();

      fetchMock
        .mockResolvedValueOnce(
          buildFetchResponse({
            status: 429,
            body: {},
            headers: { 'Retry-After': '2' },
          }),
        )
        .mockResolvedValueOnce(
          buildFetchResponse({ status: 200, body: { id: 'ok' } }),
        );

      const promise = service.get<any>('/api/v1/contacts/x', { noCache: true });

      // Drain the 2_000ms timer used between retries.
      await jest.advanceTimersByTimeAsync(2_000);

      const result = await promise;
      expect(result).toEqual({ id: 'ok' });
      expect(fetchMock).toHaveBeenCalledTimes(2);

      jest.useRealTimers();
    });
  });

  // EVO-1272: pins the HTTP contract the Journey "Move to Pipeline Stage" node
  // depends on. Mocking the client method elsewhere can't catch a URL/param/
  // envelope drift between evo-flow and the Rails endpoint — this can.
  describe('moveToPipelineStage — Journey move node contract', () => {
    it('PATCHes /pipeline_items/move_conversation with conversation_id + pipeline_stage_id', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({
          status: 200,
          body: {
            success: true,
            data: { moved: true, movement_type: 'cross_pipeline' },
          },
        }),
      );

      const result = await service.moveToPipelineStage('p1', 'conv-1', 'st9');

      const [url, init] = fetchMock.mock.calls[0];
      expect(url).toBe(
        'http://crm-test.local/api/v1/pipelines/p1/pipeline_items/move_conversation',
      );
      expect(init.method).toBe('PATCH');
      expect(JSON.parse(init.body)).toEqual({
        conversation_id: 'conv-1',
        pipeline_stage_id: 'st9',
      });
      // The move result is nested one level under the success_response envelope.
      expect(result.success).toBe(true);
      expect(result.data.data.movement_type).toBe('cross_pipeline');
    });
  });

  // EVO-2203: the three pipeline nodes reach the CRM through executeRequest, not
  // through the generic path above. This is where an archived-pipeline refusal has
  // to become a readable reason — the node copies this string into its error result.
  describe('pipeline node path — archived-pipeline refusal', () => {
    const archivedEnvelope = {
      success: false,
      error: {
        code: 'PIPELINE_ARCHIVED',
        message: 'Pipeline is archived and cannot receive conversations',
      },
      meta: { timestamp: '2026-07-24T00:00:00Z' },
    };

    it('addToPipeline reports the code and the reason, without the raw envelope', async () => {
      fetchMock.mockResolvedValue(
        buildFetchResponse({ status: 422, body: archivedEnvelope }),
      );

      const result = await service.addToPipeline('p1', 'conv-1', 'st1');

      expect(result.success).toBe(false);
      expect(result.error).toBe(
        'CRM Validation error: PIPELINE_ARCHIVED: Pipeline is archived and cannot receive conversations',
      );
      // A refusal is final: retrying it would just archive-reject three times.
      expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('moveToPipelineStage reports the same refusal', async () => {
      fetchMock.mockResolvedValue(
        buildFetchResponse({ status: 422, body: archivedEnvelope }),
      );

      const result = await service.moveToPipelineStage('p1', 'conv-1', 'st9');

      expect(result.success).toBe(false);
      expect(result.error).toBe(
        'CRM Validation error: PIPELINE_ARCHIVED: Pipeline is archived and cannot receive conversations',
      );
    });

    it('falls back to the raw body when a 422 is not the CRM envelope', async () => {
      fetchMock.mockResolvedValue(
        buildFetchResponse({ status: 422, body: 'plain text failure' }),
      );

      const result = await service.addToPipeline('p1', 'conv-1');

      expect(result.success).toBe(false);
      expect(result.error).toBe('CRM Validation error: plain text failure');
    });
  });

  // EVO-1273: pins the HTTP contract the Journey "Create Pipeline Task" node
  // depends on (URL, method, body and the nested envelope).
  describe('createPipelineTask — Journey create-task node contract', () => {
    it('POSTs /pipeline_tasks/for_conversation with conversation_id + task fields', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({
          status: 201,
          body: { success: true, data: { created: true, task_id: 'task-1' } },
        }),
      );

      const result = await service.createPipelineTask('conv-1', {
        title: 'Call lead',
        priority: 'high',
        due_in: '2.hours',
      });

      const [url, init] = fetchMock.mock.calls[0];
      expect(url).toBe(
        'http://crm-test.local/api/v1/pipeline_tasks/for_conversation',
      );
      expect(init.method).toBe('POST');
      expect(JSON.parse(init.body)).toEqual({
        conversation_id: 'conv-1',
        title: 'Call lead',
        priority: 'high',
        due_in: '2.hours',
      });
      expect(result.success).toBe(true);
      expect(result.data.data.task_id).toBe('task-1');
    });
  });

  // CRM-209: pins the flat-endpoint URL the Journey/Campaign template node depends
  // on — node specs mock this client, so only this test catches a URL drift.
  describe('getInboxMessageTemplates — Journey/Campaign template node contract', () => {
    it('GETs the flat /message_templates?inbox_id=... endpoint, not the removed nested route', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({
          status: 200,
          body: { success: true, data: [{ id: 'tpl-1', name: 'welcome' }] },
        }),
      );

      const result = await service.getInboxMessageTemplates('inbox-1');

      const [url, init] = fetchMock.mock.calls[0];
      expect(url).toBe(
        'http://crm-test.local/api/v1/message_templates?inbox_id=inbox-1&active=true&per_page=-1',
      );
      expect(init.method).toBe('GET');
      // Guard against the removed EVO-1716 nested route re-appearing.
      expect(url).not.toContain('/inboxes/inbox-1/message_templates');
      // Envelope: templates land under data (data.data at the node); resolveTemplate reads it.
      expect(result.success).toBe(true);
      expect(result.data.data[0].id).toBe('tpl-1');
    });
  });

  describe('auth headers', () => {
    it('uses X-Service-Token header by default (s2s)', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({ status: 200, body: {} }),
      );

      await service.get('/api/v1/contacts/abc');

      const [, init] = fetchMock.mock.calls[0];
      expect(init.headers['X-Service-Token']).toBe('svc-token');
      expect(init.headers['Authorization']).toBeUndefined();
    });

    it('uses Authorization: Bearer when opts.authToken is provided', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({ status: 200, body: {} }),
      );

      await service.get('/api/v1/contacts/abc', { authToken: 'usr-jwt' });

      const [, init] = fetchMock.mock.calls[0];
      expect(init.headers['Authorization']).toBe('Bearer usr-jwt');
      expect(init.headers['X-Service-Token']).toBeUndefined();
    });

    it('propagates transactionId as X-Request-Id when provided via opts', async () => {
      fetchMock.mockResolvedValueOnce(
        buildFetchResponse({ status: 200, body: {} }),
      );

      await service.get('/api/v1/contacts/abc', {
        transactionId: 'tx-xyz',
      });

      const [, init] = fetchMock.mock.calls[0];
      expect(init.headers['X-Request-Id']).toBe('tx-xyz');
    });
  });
  describe('outbound_headers', () => {
    beforeEach(() => {
      EvoExtensionPoints.replace('outbound_headers', () => ({
        'x-ctx': 'from-overlay',
        'X-Service-Token': 'must-not-win',
      }));
      fetchMock.mockResolvedValue(
        buildFetchResponse({ status: 200, body: {} }),
      );
    });
    afterEach(() => EvoExtensionPoints.reset());

    const sentHeaders = (): Record<string, string> =>
      (fetchMock.mock.calls[0][1] as { headers: Record<string, string> })
        .headers;

    it('rides the generic request path', async () => {
      await service.get('/api/v1/contacts/c-1');

      expect(sentHeaders()).toMatchObject({
        'x-ctx': 'from-overlay',
        'X-Service-Token': 'svc-token',
      });
    });

    it('rides the legacy request path used by the journey nodes', async () => {
      await service.getConversation({ conversationId: 'conv-1' } as never);

      expect(sentHeaders()).toMatchObject({
        'x-ctx': 'from-overlay',
        'X-Service-Token': 'svc-token',
      });
    });
  });
});
