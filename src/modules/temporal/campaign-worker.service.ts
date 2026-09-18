import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Worker, NativeConnection } from '@temporalio/worker';
import { DataSource } from 'typeorm';
import { campaignExecutionActivities } from './activities/campaign-execution.activities';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';
import { AppFactory } from '../../app-factory';
import { EvoExtensionPoints } from '../../evo-extension-points/registry';

/**
 * Campaign Temporal Worker Service
 * Handles campaign execution workflows and activities
 * Separate from Journey worker for independent scaling
 */
@Injectable()
export class CampaignWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new CustomLoggerService(CampaignWorkerService.name);
  private worker: Worker | null = null;
  private connection: NativeConnection | null = null;
  private isInitializing: boolean = false;
  private retryAttempts: number = 0;
  private maxRetries: number = 5;

  constructor(private readonly dataSource: DataSource) {}

  async onModuleInit() {
    // Only start campaign worker in campaign-enabled modes
    if (!AppFactory.shouldStartCampaignWorker()) {
      this.logger.log(
        'Campaign worker disabled for current RUN_MODE',
      );
      return;
    }

    this.logger.log('Initializing Campaign Temporal Worker...');

    try {
      await this.startWorker();
    } catch (error) {
      this.logger.error('Failed to start campaign worker on init', {
        error: error.message,
      });
      this.scheduleRetry();
    }
  }

  private async startWorker(): Promise<void> {
    if (this.isInitializing) {
      this.logger.warn('Campaign worker is already initializing, skipping...');
      return;
    }

    this.isInitializing = true;
    const temporalAddress = process.env.TEMPORAL_ADDRESS || 'localhost:7233';

    this.logger.log('Starting Campaign Temporal worker...', {
      temporalAddress,
      taskQueue: 'campaign-execution',
      attempt: this.retryAttempts + 1,
    });

    try {
      // Create connection to Temporal server
      this.connection = await NativeConnection.connect({
        address: temporalAddress,
      });

      this.logger.log('Connected to Temporal server successfully');

      // Create worker with campaign workflows and activities
      this.worker = await Worker.create({
        connection: this.connection,
        namespace: 'default',
        taskQueue: 'campaign-execution',
        workflowsPath: require.resolve(
          './workflows/campaign-execution.workflow',
        ),
        activities: {
          ...campaignExecutionActivities,
        },
        interceptors: EvoExtensionPoints.get('temporal_interceptors')(
          this.dataSource,
        ).worker,
        reuseV8Context: true,
        maxConcurrentActivityTaskExecutions: 100,
        maxConcurrentWorkflowTaskExecutions: 100,
        stickyQueueScheduleToStartTimeout: '10s',
        maxActivitiesPerSecond: 50,
        maxTaskQueueActivitiesPerSecond: 500,
      });

      this.logger.log('Campaign Temporal worker created successfully', {
        taskQueue: 'campaign-execution',
        workflowsRegistered: [
          'CampaignExecutionWorkflow',
          'CampaignTestExecutionWorkflow',
        ],
        activitiesRegistered: Object.keys(campaignExecutionActivities),
      });

      // Reset retry counter on success
      this.retryAttempts = 0;

      // Start worker in background
      this.startWorkerInBackground();
    } catch (error) {
      this.logger.error('Failed to create/start Campaign Temporal worker', {
        error: error.message,
        stack: error.stack,
        temporalAddress,
        attempt: this.retryAttempts + 1,
      });

      this.isInitializing = false;
      throw error;
    }

    this.isInitializing = false;
  }

  private startWorkerInBackground(): void {
    if (!this.worker) {
      this.logger.error('Cannot start worker - worker not initialized');
      return;
    }

    // Start worker in background without blocking
    this.worker
      .run()
      .then(() => {
        this.logger.log('✅ Campaign Temporal worker started and running');
      })
      .catch((error) => {
        this.logger.error('Campaign Temporal worker failed', {
          error: error.message,
          stack: error.stack,
        });

        // Schedule retry on failure
        this.scheduleRetry();
      });
  }

  private scheduleRetry(): void {
    if (this.retryAttempts >= this.maxRetries) {
      this.logger.error(
        `Campaign worker failed after ${this.maxRetries} attempts, giving up`,
      );
      return;
    }

    this.retryAttempts++;
    const retryDelay = Math.min(5000 * Math.pow(2, this.retryAttempts - 1), 60000);

    this.logger.log(`Scheduling campaign worker retry in ${retryDelay}ms`, {
      attempt: this.retryAttempts,
      maxRetries: this.maxRetries,
    });

    setTimeout(() => {
      this.logger.log(`Retrying campaign worker start (attempt ${this.retryAttempts})...`);
      this.startWorker().catch((error) => {
        this.logger.error('Campaign worker retry failed', {
          error: error.message,
        });
        this.scheduleRetry();
      });
    }, retryDelay);
  }

  async onModuleDestroy() {
    this.logger.log('Shutting down Campaign Temporal worker...');

    if (this.worker) {
      try {
        await this.worker.shutdown();
        this.logger.log('Campaign worker shut down successfully');
      } catch (error) {
        this.logger.error('Error shutting down campaign worker', {
          error: error.message,
        });
      }
    }

    if (this.connection) {
      try {
        await this.connection.close();
        this.logger.log('Campaign Temporal connection closed successfully');
      } catch (error) {
        this.logger.error('Error closing campaign Temporal connection', {
          error: error.message,
        });
      }
    }
  }
}
