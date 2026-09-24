// The node builds CrmClientService in its constructor, which throws without a token.
process.env.EVOAI_CRM_API_TOKEN ||= 'test-token';
process.env.EVOAI_CRM_BASE_URL ||= 'http://crm-test.local';

import { AssignAgentNode } from './assign-agent.node';

describe('AssignAgentNode', () => {
  it('EVO-1741: skips with a visible failure when the trigger provides no conversation', async () => {
    const node = new AssignAgentNode();
    const assignAgent = jest.fn();
    (node as any).crmService = { assignAgent };

    const result = await node.execute({
      nodeId: 'n1',
      conversationId: '',
      sessionId: 's1',
      nodeData: { agent_id: 'a1' },
    });

    expect(assignAgent).not.toHaveBeenCalled();
    expect(result.success).toBe(false);
    expect(result.skipped).toBe(true);
    expect(result.error).toContain('no_conversation_id');
  });
});
