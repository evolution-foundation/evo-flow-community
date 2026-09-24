// The node builds CrmClientService in its constructor, which throws without a token.
process.env.EVOAI_CRM_API_TOKEN ||= 'test-token';
process.env.EVOAI_CRM_BASE_URL ||= 'http://crm-test.local';

import { MuteConversationNode } from './mute-conversation.node';

describe('MuteConversationNode', () => {
  it('EVO-1741: skips with a visible failure when the trigger provides no conversation', async () => {
    const node = new MuteConversationNode();
    const muteConversation = jest.fn();
    (node as any).crmService = { muteConversation };

    const result = await node.execute({
      nodeId: 'n1',
      conversationId: '',
      sessionId: 's1',
      nodeData: {},
    });

    expect(muteConversation).not.toHaveBeenCalled();
    expect(result.success).toBe(false);
    expect(result.skipped).toBe(true);
    expect(result.error).toContain('no_conversation_id');
  });
});
