import { log } from '@temporalio/activity';
import type { INestApplicationContext } from '@nestjs/common';
import { getAppContext as getHeldContext } from '../../../shared/app-context.holder';
import { AudienceComputationService } from '../../../shared/audience/audience-computation.service';
import { CampaignsService } from '../../campaigns/services/campaigns.service';
import { Campaign } from '../../campaigns/entities/campaign.entity';
import { CampaignExecution } from '../../campaigns/entities/campaign-execution.entity';
import { CampaignContactStatus } from '../../campaigns/entities/campaign-contact.entity';
import { runActivityInTenantDbContext } from '../tenant-activity-context';
import { TenantDbContext } from '../../../evo-extension-points';
import {
  IMESSAGE_BROKER,
  IMessageBroker,
} from '../../../shared/broker/interfaces/message-broker.interface';
import {
  CAMPAIGNS_PACK_TOPIC,
  CampaignsPackContract,
} from '../../../shared/broker/contracts/campaigns-pack.contract';

// EVO-1829: reuse the primary app context (stashed at boot) instead of
// bootstrapping a second AppModule, which freezes single-mode.
async function getAppContext() {
  return getHeldContext();
}

// ==================== Input/Output Interfaces ====================

export interface ComputeCampaignAudienceInput {
  campaignId: string;
}

export interface ComputeCampaignAudienceOutput {
  campaignId: string;
  totalContacts: number;
  validContacts: number;
  invalidContacts: number;
  processingTimeMs: number;
}

export interface CreateCampaignBatchesInput {
  campaignId: string;
  batchSize: number;
}

export interface CreateCampaignBatchesOutput {
  campaignId: string;
  totalBatches: number;
  totalContacts: number;
  batchSize: number;
}

export interface GetCampaignBatchInput {
  campaignId: string;
  batchNumber: number;
}

export interface CampaignBatch {
  batchNumber: number;
  contacts: Array<{
    id: string;
    campaignContactId: string;
  }>;
  totalInBatch: number;
}

export interface UpdateCampaignStatusInput {
  campaignId: string;
  status: 0 | 1 | 2 | 3 | 4 | 5 | 6;
  sentContacts?: number;
  sentPercentage?: number;
}

export interface GetCampaignDataInput {
  campaignId: string;
}

export interface PublishCampaignsPackInput {
  campaignId: string;
  correlationId: string;
}

export interface UpdateExecutionProgressInput {
  campaignId: string;
  workflowId: string;
  /**
   * Tenant the workflow belongs to (ADR14, story 10.1b). Propagated from the
   * workflow payload so the activity's DB write is scoped by RLS. Optional for
   * single-tenant (resolves to DEFAULT_TENANT_ID); under multi-tenant a missing
   * value makes the activity fail explicitly rather than run unscoped.
   */
  tenantId?: string | null;
  status?: 'running' | 'paused' | 'completed' | 'failed' | 'cancelled';
  totalContacts?: number;
  processedContacts?: number;
  sentContacts?: number;
  failedContacts?: number;
  currentBatch?: number;
  totalBatches?: number;
  lastError?: string;
}

// ==================== Activities Interface ====================

export interface CampaignExecutionActivities {
  computeCampaignAudience(
    input: ComputeCampaignAudienceInput,
  ): Promise<ComputeCampaignAudienceOutput>;

  createCampaignBatches(
    input: CreateCampaignBatchesInput,
  ): Promise<CreateCampaignBatchesOutput>;

  getCampaignBatch(input: GetCampaignBatchInput): Promise<CampaignBatch>;

  updateCampaignStatus(input: UpdateCampaignStatusInput): Promise<void>;

  getCampaignData(input: GetCampaignDataInput): Promise<Campaign>;

  publishCampaignsPack(input: PublishCampaignsPackInput): Promise<void>;

  updateExecutionProgress(input: UpdateExecutionProgressInput): Promise<void>;

  markBatchAsProcessed(
    campaignId: string,
    batchNumber: number,
  ): Promise<void>;
}

// ==================== Activity Implementations ====================

/**
 * Compute campaign audience by running segmentation
 */
export async function computeCampaignAudience(
  input: ComputeCampaignAudienceInput,
): Promise<ComputeCampaignAudienceOutput> {
  log.info('Computing campaign audience', {
    campaignId: input.campaignId,
  });

  try {
    const app = await getAppContext();
    const audienceService = app.get(AudienceComputationService);

    const result = await audienceService.computeAudience(
      input.campaignId,
    );

    log.info('Campaign audience computed successfully', {
      campaignId: input.campaignId,
      totalContacts: result.totalContacts,
      validContacts: result.validContacts,
    });

    return result;
  } catch (error) {
    log.error('Failed to compute campaign audience', {
      campaignId: input.campaignId,
      error: error.message,
      stack: error.stack,
    });
    throw error;
  }
}

/**
 * Create batches for campaign execution
 */
export async function createCampaignBatches(
  input: CreateCampaignBatchesInput,
): Promise<CreateCampaignBatchesOutput> {
  log.info('Creating campaign batches', {
    campaignId: input.campaignId,
    batchSize: input.batchSize,
  });

  try {
    const app = await getAppContext();
    const audienceService = app.get(AudienceComputationService);

    // Assign batch sequences
    const result = await audienceService.assignBatchSequences(
      input.campaignId,
      input.batchSize,
    );

    // Get total audience count
    const totalContacts = await audienceService.getAudienceCount(
      input.campaignId,
    );

    log.info('Campaign batches created successfully', {
      campaignId: input.campaignId,
      totalBatches: result.batches,
      totalContacts,
    });

    return {
      campaignId: input.campaignId,
      totalBatches: result.batches,
      totalContacts,
      batchSize: input.batchSize,
    };
  } catch (error) {
    log.error('Failed to create campaign batches', {
      campaignId: input.campaignId,
      error: error.message,
      stack: error.stack,
    });
    throw error;
  }
}

/**
 * Get contacts for a specific batch
 */
export async function getCampaignBatch(
  input: GetCampaignBatchInput,
): Promise<CampaignBatch> {
  log.debug('Getting campaign batch', {
    campaignId: input.campaignId,
    batchNumber: input.batchNumber,
  });

  try {
    const app = await getAppContext();
    const dataSource = app.get('DataSource');

    // Query campaigns_contacts for specific batch
    const contacts = await dataSource
      .getRepository('CampaignContact')
      .createQueryBuilder('cc')
      .select(['cc.id', 'cc.contactId'])
      .where('cc.campaignId = :campaignId', { campaignId: input.campaignId })
      
      .andWhere('cc.batchSequence = :batchNumber', {
        batchNumber: input.batchNumber,
      })
      .andWhere('cc.status = :status', {
        status: CampaignContactStatus.PENDING,
      })
      .getMany();

    log.debug('Campaign batch retrieved', {
      campaignId: input.campaignId,
      batchNumber: input.batchNumber,
      contactCount: contacts.length,
    });

    return {
      batchNumber: input.batchNumber,
      contacts: contacts.map((c: any) => ({
        id: c.contactId,
        campaignContactId: c.id,
      })),
      totalInBatch: contacts.length,
    };
  } catch (error) {
    log.error('Failed to get campaign batch', {
      campaignId: input.campaignId,
      batchNumber: input.batchNumber,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Update campaign status
 */
export async function updateCampaignStatus(
  input: UpdateCampaignStatusInput,
): Promise<void> {
  log.info('Updating campaign status', {
    campaignId: input.campaignId,
    status: input.status,
  });

  try {
    const app = await getAppContext();
    const campaignRepo = app.get(TenantDbContext).getRepository(Campaign);

    // Build update object
    const updates: any = {
      status: input.status,
    };

    if (input.sentContacts !== undefined) {
      updates.sentContacts = input.sentContacts;
    }

    if (input.sentPercentage !== undefined) {
      updates.sentPercentage = input.sentPercentage;
    }

    // Update campaign
    await campaignRepo.update(
      {
        id: input.campaignId,
      },
      updates,
    );

    log.info('Campaign status updated successfully', {
      campaignId: input.campaignId,
      status: input.status,
    });
  } catch (error) {
    log.error('Failed to update campaign status', {
      campaignId: input.campaignId,
      status: input.status,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Get campaign data
 */
export async function getCampaignData(
  input: GetCampaignDataInput,
): Promise<Campaign> {
  log.debug('Getting campaign data', {
    campaignId: input.campaignId,
  });

  try {
    const app = await getAppContext();
    const campaignsService = app.get(CampaignsService);

    const campaign = await campaignsService.findOne(
      input.campaignId,
    );

    return campaign;
  } catch (error) {
    log.error('Failed to get campaign data', {
      campaignId: input.campaignId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Mark batch as processed
 */
export async function markBatchAsProcessed(
  campaignId: string,
  batchNumber: number,
): Promise<void> {
  log.debug('Marking batch as processed', {
    campaignId,
    batchNumber,
  });

  try {
    const app = await getAppContext();
    const dataSource = app.get('DataSource');

    // Update campaigns_contacts status for this batch
    await dataSource
      .getRepository('CampaignContact')
      .createQueryBuilder()
      .update()
      .set({
        status: CampaignContactStatus.SENT,
        sentAt: new Date(),
      })
      .where('campaignId = :campaignId', { campaignId })
      .andWhere('batchSequence = :batchNumber', { batchNumber })
      .execute();

    log.debug('Batch marked as processed', {
      campaignId,
      batchNumber,
    });
  } catch (error) {
    log.error('Failed to mark batch as processed', {
      campaignId,
      batchNumber,
      error: error.message,
    });
    throw error;
  }
}

export async function updateExecutionProgress(
  input: UpdateExecutionProgressInput,
): Promise<void> {
  try {
    const updates: Record<string, any> = {};
    if (input.status !== undefined) updates.status = input.status;
    if (input.totalContacts !== undefined) updates.totalContacts = input.totalContacts;
    if (input.processedContacts !== undefined) updates.processedContacts = input.processedContacts;
    if (input.sentContacts !== undefined) updates.sentContacts = input.sentContacts;
    if (input.failedContacts !== undefined) updates.failedContacts = input.failedContacts;
    if (input.currentBatch !== undefined) updates.currentBatch = input.currentBatch;
    if (input.totalBatches !== undefined) updates.totalBatches = input.totalBatches;
    if (input.lastError !== undefined) updates.lastError = input.lastError;
    if (input.status === 'completed' || input.status === 'failed' || input.status === 'cancelled') {
      updates.endedAt = new Date();
    }

    // Reference wiring (ADR14, story 10.1b): scope the activity's DB write to the
    // workflow's tenant via the seam. Uses the handed EntityManager so the UPDATE
    // runs on the connection carrying app.current_tenant_id (enterprise), or the
    // global pool (community / single-tenant). A missing tenant under multi-tenant
    // throws here instead of writing across tenants.
    await runActivityInTenantDbContext(input.tenantId, (manager) =>
      manager.getRepository(CampaignExecution).update(
        {
          campaignId: input.campaignId,
          workflowId: input.workflowId,
        },
        updates,
      ),
    );
  } catch (error) {
    log.error('Failed to update campaign execution progress', {
      campaignId: input.campaignId,
      workflowId: input.workflowId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Switch-flip of the distributed campaign pipeline (story 4.7 / EVO-1221):
 * publishes a single `campaigns.pack` message and returns once the broker
 * acks, handing the heavy lifting (audience, pagination, dispatch, tracking)
 * to the packer/sender/tracker workers. Replaced the former inline dispatch
 * (the CampaignMessageSenderService batch loop, removed in story 5.5 / EVO-1227).
 *
 * `triggeredBy` is fixed to `'schedule'`: the landed contract enum
 * (`campaigns-pack.contract.ts`, story 1.5) is `schedule|manual|recurrence`
 * and the packer only reads `campaignId`/`correlationId`, so the field is
 * pure provenance — the Temporal trigger fires when the campaign schedule
 * expires. A broker error propagates so Temporal applies the activity proxy's
 * default retry policy (the broker is the fault, not the workflow).
 */
export async function publishCampaignsPack(
  input: PublishCampaignsPackInput,
): Promise<void> {
  const { campaignId, correlationId } = input;

  log.info('Publishing campaigns.pack', { campaignId, correlationId });

  const app = (await getAppContext()) as INestApplicationContext;
  const broker = app.get<IMessageBroker>(IMESSAGE_BROKER);

  const payload: CampaignsPackContract = {
    campaignId,
    triggeredAt: new Date().toISOString(),
    triggeredBy: 'schedule',
    correlationId,
  };

  await broker.publish(CAMPAIGNS_PACK_TOPIC, payload);

  log.info('campaigns.pack published', { campaignId, correlationId });
}

// Export activities object for worker registration
export const campaignExecutionActivities: CampaignExecutionActivities = {
  computeCampaignAudience,
  createCampaignBatches,
  getCampaignBatch,
  updateCampaignStatus,
  getCampaignData,
  publishCampaignsPack,
  updateExecutionProgress,
  markBatchAsProcessed,
};
