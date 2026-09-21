import {
  publishCampaignsPack,
  updateCampaignStatus,
} from './campaign-execution.activities';
import { EntityManager } from 'typeorm';
import { ClsServiceManager } from 'nestjs-cls';
import {
  EvoExtensionPoints,
  TENANT_DB_MANAGER_CLS_KEY,
} from '../../../evo-extension-points';
import { Campaign } from '../../campaigns/entities/campaign.entity';
import { IMESSAGE_BROKER } from '../../../shared/broker/interfaces/message-broker.interface';
import {
  CAMPAIGNS_PACK_TOPIC,
  CampaignsPackContract,
  isCampaignsPackContract,
} from '../../../shared/broker/contracts/campaigns-pack.contract';

// EVO-1829: the activity resolves services from the primary app context held in
// app-context.holder (no second AppModule bootstrap). Mock the holder so the
// unit test never pulls the real application graph (DB, brokers) in.
const mockAppGet = jest.fn();
jest.mock('../../../shared/app-context.holder', () => ({
  getAppContext: () => ({ get: mockAppGet }),
}));
jest.mock('../../../database/ormconfig', () => ({
  AppDataSource: { isInitialized: true },
}));
jest.mock('@temporalio/activity', () => ({
  log: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() },
}));

const CORRELATION_ID = '11111111-1111-4111-8111-111111111111';
const CAMPAIGN_ID = 'camp-1';

describe('publishCampaignsPack activity', () => {
  // The broker is resolved via the reused mockAppGet/publish jest fns: the mock
  // holder returns a fresh context object each call (not a singleton), while the
  // production holder IS a genuine singleton. Reconfigure per test on `publish`.
  const publish = jest.fn();

  beforeEach(() => {
    publish.mockReset().mockResolvedValue(undefined);
    mockAppGet.mockReset().mockReturnValue({ publish });
  });

  it('publishes one schema-valid campaigns.pack message resolved via the broker token', async () => {
    await publishCampaignsPack({
      campaignId: CAMPAIGN_ID,
      correlationId: CORRELATION_ID,
    });

    expect(mockAppGet).toHaveBeenCalledWith(IMESSAGE_BROKER);
    expect(publish).toHaveBeenCalledTimes(1);

    const [topic, payload] = publish.mock.calls[0] as [
      string,
      CampaignsPackContract,
    ];
    expect(topic).toBe(CAMPAIGNS_PACK_TOPIC);
    expect(payload).toMatchObject({
      campaignId: CAMPAIGN_ID,
      triggeredBy: 'schedule',
      correlationId: CORRELATION_ID,
    });
    expect(typeof payload.triggeredAt).toBe('string');
    // The published payload must satisfy the landed story-1.5 contract.
    expect(isCampaignsPackContract(payload)).toBe(true);
  });

  it('propagates a broker error so Temporal applies the activity retry policy (AC4)', async () => {
    const brokerError = new Error('broker timeout');
    publish.mockRejectedValueOnce(brokerError);

    await expect(
      publishCampaignsPack({
        campaignId: CAMPAIGN_ID,
        correlationId: CORRELATION_ID,
      }),
    ).rejects.toThrow('broker timeout');
  });
});

describe('updateCampaignStatus activity', () => {
  afterEach(() => EvoExtensionPoints.reset());

  it('writes on the manager an activity interceptor bound for the tenant', async () => {
    const update = jest.fn().mockResolvedValue(undefined);
    const getRepository = jest.fn().mockReturnValue({ update });
    const cls = ClsServiceManager.getClsService();

    await cls.run(() => {
      cls.set(TENANT_DB_MANAGER_CLS_KEY, { getRepository });
      return updateCampaignStatus({ campaignId: CAMPAIGN_ID, status: 2 });
    });

    expect(getRepository).toHaveBeenCalledWith(Campaign);
    expect(update).toHaveBeenCalledWith({ id: CAMPAIGN_ID }, { status: 2 });
  });

  it('asks the tenant DB seam with no tenant when nothing is bound, so the overlay can refuse', async () => {
    EvoExtensionPoints.replace('tenant_db_context', (_ds, tenantId) => {
      expect(tenantId).toBeNull();
      throw new Error('TENANT_CONTEXT_REQUIRED');
    });

    await expect(
      updateCampaignStatus({ campaignId: CAMPAIGN_ID, status: 2 }),
    ).rejects.toThrow('TENANT_CONTEXT_REQUIRED');
  });

  it('keeps writing in community, where the seam is a passthrough', async () => {
    const update = jest.fn().mockResolvedValue(undefined);
    EvoExtensionPoints.replace('tenant_db_context', (_ds, _tenantId, work) =>
      work({
        getRepository: () => ({ update }),
      } as unknown as EntityManager),
    );

    await updateCampaignStatus({ campaignId: CAMPAIGN_ID, status: 2 });

    expect(update).toHaveBeenCalledWith({ id: CAMPAIGN_ID }, { status: 2 });
  });
});
