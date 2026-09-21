import { Injectable, Logger } from '@nestjs/common';
import { Connection, Client } from '@temporalio/client';
import { ConfigService } from '@nestjs/config';
import { DataSource } from 'typeorm';
import { EvoExtensionPoints } from '../../../evo-extension-points/registry';

export interface ScheduleCampaignWorkflowInput {
  campaignId: string;
  batchSize?: number;
  delayBetweenBatches?: number;
  skipAudienceComputation?: boolean;
  scheduleAt?: Date;
}

export interface CampaignWorkflowStatus {
  workflowId: string;
  runId: string;
  status: 'running' | 'completed' | 'failed' | 'cancelled' | 'terminated';
}

const PAUSE_CAMPAIGN_SIGNAL = 'pauseCampaign';
const RESUME_CAMPAIGN_SIGNAL = 'resumeCampaign';
const CANCEL_CAMPAIGN_SIGNAL = 'cancelCampaign';

/**
 * Service for scheduling and managing campaign execution workflows
 * Integrates with Temporal.io for workflow orchestration
 */
@Injectable()
export class CampaignWorkflowService {
  private readonly logger = new Logger(CampaignWorkflowService.name);
  private client: Client | null = null;
  private connection: Connection | null = null;

  constructor(
    private readonly configService: ConfigService,
    private readonly dataSource: DataSource,
  ) {}

  /**
   * Get or create Temporal client connection
   */
  private async getClient(): Promise<Client> {
    if (this.client) {
      return this.client;
    }

    try {
      const temporalAddress =
        this.configService.get<string>('TEMPORAL_ADDRESS') ||
        'localhost:7233';

      this.logger.log(`Connecting to Temporal at ${temporalAddress}`);

      // Create connection to Temporal server
      this.connection = await Connection.connect({
        address: temporalAddress,
      });

      // Create client
      this.client = new Client({
        connection: this.connection,
        namespace: 'default',
        interceptors: EvoExtensionPoints.get('temporal_interceptors')(
          this.dataSource,
        ).client,
      });

      this.logger.log('Successfully connected to Temporal');

      return this.client;
    } catch (error) {
      this.logger.error('Failed to connect to Temporal', {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Schedule a campaign workflow execution
   */
  async scheduleCampaignExecution(
    input: ScheduleCampaignWorkflowInput,
  ): Promise<{ workflowId: string; runId: string }> {
    this.logger.log('Scheduling campaign workflow', {
      campaignId: input.campaignId,
      scheduleAt: input.scheduleAt,
    });

    try {
      const client = await this.getClient();

      // Generate unique workflow ID
      const workflowId = `campaign-${input.campaignId}-${Date.now()}`;

      // Prepare workflow input
      const workflowInput = {
        campaignId: input.campaignId,
        batchSize: input.batchSize || 1000,
        delayBetweenBatches: input.delayBetweenBatches || 0,
        skipAudienceComputation: input.skipAudienceComputation || false,
      };

      // Start workflow
      const handle = await client.workflow.start('CampaignExecutionWorkflow', {
        taskQueue: 'campaign-execution',
        workflowId,
        args: [workflowInput],
        // If scheduled for future, use startDelay
        ...(input.scheduleAt && {
          startDelay: Math.max(
            0,
            input.scheduleAt.getTime() - Date.now(),
          ),
        }),
      });

      this.logger.log('Campaign workflow scheduled successfully', {
        campaignId: input.campaignId,
        workflowId: handle.workflowId,
        runId: handle.firstExecutionRunId,
      });

      return {
        workflowId: handle.workflowId,
        runId: handle.firstExecutionRunId,
      };
    } catch (error) {
      this.logger.error('Failed to schedule campaign workflow', {
        campaignId: input.campaignId,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Start campaign execution immediately
   */
  async startCampaignExecution(
    campaignId: string,
    options?: {
      batchSize?: number;
      delayBetweenBatches?: number;
      skipAudienceComputation?: boolean;
    },
  ): Promise<{ workflowId: string; runId: string }> {
    return this.scheduleCampaignExecution({
      campaignId,
      batchSize: options?.batchSize,
      delayBetweenBatches: options?.delayBetweenBatches,
      skipAudienceComputation: options?.skipAudienceComputation,
    });
  }

  /**
   * Get workflow status
   */
  async getWorkflowStatus(
    workflowId: string,
  ): Promise<CampaignWorkflowStatus> {
    try {
      const client = await this.getClient();

      const handle = client.workflow.getHandle(workflowId);
      const description = await handle.describe();

      let status: CampaignWorkflowStatus['status'] = 'running';

      if (description.status.name === 'COMPLETED') {
        status = 'completed';
      } else if (description.status.name === 'FAILED') {
        status = 'failed';
      } else if (description.status.name === 'CANCELLED') {
        status = 'cancelled';
      } else if (description.status.name === 'TERMINATED') {
        status = 'terminated';
      }

      return {
        workflowId: description.workflowId,
        runId: description.runId,
        status,
      };
    } catch (error) {
      this.logger.error('Failed to get workflow status', {
        workflowId,
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Cancel a running campaign workflow
   */
  async cancelWorkflow(workflowId: string): Promise<void> {
    this.logger.log('Cancelling campaign workflow', { workflowId });

    try {
      const client = await this.getClient();

      const handle = client.workflow.getHandle(workflowId);
      await handle.signal(CANCEL_CAMPAIGN_SIGNAL);
      await handle.cancel();

      this.logger.log('Campaign workflow cancelled successfully', {
        workflowId,
      });
    } catch (error) {
      this.logger.error('Failed to cancel workflow', {
        workflowId,
        error: error.message,
      });
      throw error;
    }
  }

  async pauseWorkflow(workflowId: string): Promise<void> {
    this.logger.log('Pausing campaign workflow', { workflowId });
    const client = await this.getClient();
    const handle = client.workflow.getHandle(workflowId);
    await handle.signal(PAUSE_CAMPAIGN_SIGNAL);
  }

  async resumeWorkflow(workflowId: string): Promise<void> {
    this.logger.log('Resuming campaign workflow', { workflowId });
    const client = await this.getClient();
    const handle = client.workflow.getHandle(workflowId);
    await handle.signal(RESUME_CAMPAIGN_SIGNAL);
  }

  /**
   * Get workflow result (waits for completion)
   */
  async getWorkflowResult(workflowId: string): Promise<any> {
    try {
      const client = await this.getClient();

      const handle = client.workflow.getHandle(workflowId);
      const result = await handle.result();

      return result;
    } catch (error) {
      this.logger.error('Failed to get workflow result', {
        workflowId,
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Cleanup - close connections
   */
  async onModuleDestroy() {
    if (this.connection) {
      await this.connection.close();
      this.logger.log('Temporal connection closed');
    }
  }
}
