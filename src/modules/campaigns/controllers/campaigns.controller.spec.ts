import {
  BadRequestException,
  ConflictException,
} from '@nestjs/common';
import { CampaignsController } from './campaigns.controller';

describe('CampaignsController critical execution flows', () => {
  const accountId = 'acc-1';
  const campaignId = 'camp-1';

  const buildController = () => {
    const campaignsService: any = {
      findOne: jest.fn().mockResolvedValue({ id: campaignId }),
      pause: jest.fn(),
      resume: jest.fn(),
      stop: jest.fn().mockResolvedValue({ id: campaignId, status: 'stopped' }),
    };

    const audienceComputationService: any = {};
    const audienceValidationService: any = {};
    const campaignWorkflowService: any = {
      startCampaignExecution: jest.fn().mockResolvedValue({
        workflowId: 'wf-1',
        runId: 'run-1',
      }),
      cancelWorkflow: jest.fn().mockResolvedValue(undefined),
      pauseWorkflow: jest.fn().mockResolvedValue(undefined),
      resumeWorkflow: jest.fn().mockResolvedValue(undefined),
    };
    const campaignExecutionsService: any = {
      getActiveExecution: jest.fn().mockResolvedValue(null),
      createExecution: jest.fn().mockResolvedValue({ id: 'exec-1' }),
      updateStatus: jest.fn().mockResolvedValue(undefined),
      getLatestExecution: jest.fn().mockResolvedValue(null),
    };
    const cls: any = {
      get: jest.fn().mockReturnValue(accountId),
    };

    const controller = new CampaignsController(
      campaignsService,
      audienceComputationService,
      audienceValidationService,
      campaignWorkflowService,
      campaignExecutionsService,
      cls,
    );

    return {
      controller,
      campaignsService,
      campaignWorkflowService,
      campaignExecutionsService,
      cls,
    };
  };

  it('cancels started workflow when persistence of execution fails', async () => {
    const {
      controller,
      campaignWorkflowService,
      campaignExecutionsService,
    } = buildController();

    campaignExecutionsService.createExecution.mockRejectedValueOnce(
      new Error('db error'),
    );

    await expect(controller.executeCampaign(campaignId, {})).rejects.toBeInstanceOf(
      ConflictException,
    );
    expect(campaignWorkflowService.startCampaignExecution).toHaveBeenCalledTimes(
      1,
    );
    expect(campaignWorkflowService.cancelWorkflow).toHaveBeenCalledWith('wf-1');
  });

  it('rejects cancelExecution when no active execution and no workflow_id', async () => {
    const { controller } = buildController();

    await expect(
      controller.cancelExecution(campaignId, {}),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it('rejects pause when there is no active execution', async () => {
    const { controller } = buildController();

    await expect(controller.pause(campaignId)).rejects.toBeInstanceOf(
      BadRequestException,
    );
  });

  it('keeps backward compatibility: stop works without active execution', async () => {
    const { controller, campaignsService } = buildController();

    const result = await controller.stop(campaignId);
    expect(campaignsService.stop).toHaveBeenCalledWith(campaignId);
    expect(result).toEqual({ id: campaignId, status: 'stopped' });
  });
});

