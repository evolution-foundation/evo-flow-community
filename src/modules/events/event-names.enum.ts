// Canonical list of event names emitted by the CRM publisher to evo-flow.
// Mirror of evo-ai-crm-community/lib/events/evo_flow_event_names.rb
// (EvoFlow::EVENT_NAMES). The script `scripts/check-event-names-sync.sh` at
// the monorepo root enforces sync between both files in CI — keep them
// in lockstep when adding/removing entries.
//
// One string per line is required by the script's regex extraction.
export const EVENT_NAMES = [
  'contact.created',
  'contact.updated',
  'contact.deleted',
  'contact.label.added',
  'contact.label.removed',
  'contact.custom_attribute.changed',
  'conversation.created',
  'conversation.resolved',
  'conversation.activity',
  'conversation.first_reply',
  'conversation.reply_time',
  'conversation.bot_handoff',
  'conversation.bot_resolved',
  'message.created',
  'message.delivered',
  'message.read',
  'message.failed',
  'campaign.triggered',
  'campaign.message.sent',
  'campaign.message.opened',
  'campaign.message.clicked',
  'pipeline.stage_changed',
  'purchase.approved',
  'scheduled_action.executed',
  'scheduled_action.failed',
  'custom',
] as const;

export type EvoFlowEventName = (typeof EVENT_NAMES)[number];
