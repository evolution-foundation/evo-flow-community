import { EVENT_NAMES, type EvoFlowEventName } from '../event-names.enum';
import type {
  EventCatalog,
  EventCatalogEntry,
  EventCategory,
  FieldSpec,
} from './event-schema.types';

const f = (type: FieldSpec['type'], description?: string): FieldSpec => ({ type, description });

const contactIdentityOptionalFields: Record<string, FieldSpec> = {
  name: f('string'),
  email: f('string'),
  phone_number: f('string'),
  identifier: f('string'),
  middle_name: f('string'),
  last_name: f('string'),
  location: f('string'),
  country_code: f('string'),
  contact_type: f('string'),
  blocked: f('boolean'),
  created_at: f('date'),
  updated_at: f('date'),
  customAttributes: f('object'),
  additionalAttributes: f('object'),
  created_via: f('string'),
};

const messageCommonRequired: Record<string, FieldSpec> = {
  message_id: f('uuid', 'Message UUID'),
  channel_type: f('string', 'Channel identifier (e.g., Channel::Whatsapp)'),
  conversation_id: f('uuid'),
  source: f('string'),
};

const messageCommonOptional: Record<string, FieldSpec> = {
  message_type: f('string', 'incoming | outgoing'),
  content_type: f('string'),
  content: f('string', 'Truncated to 2000 chars; absent when EVO_FLOW_MESSAGE_CONTENT_DISABLED=true'),
};

const scheduledActionRequired: Record<string, FieldSpec> = {
  scheduled_action_id: f('uuid', 'Numeric id of the scheduled action'),
  action_type: f(
    'string',
    'send_message | execute_webhook | create_task | ...',
  ),
  scheduled_for: f('date', 'When the action was due'),
  source: f('string'),
};

const scheduledActionOptional: Record<string, FieldSpec> = {
  conversation_id: f('uuid'),
};

const ENTRIES: EventCatalogEntry[] = [
  {
    eventName: 'contact.created',
    category: 'contact',
    dtoType: 'identify',
    labelPt: 'Contato criado',
    labelEn: 'Contact created',
    description: 'A new contact was created in the CRM.',
    schema: {
      required: { id: f('uuid'), source: f('string') },
      optional: contactIdentityOptionalFields,
    },
  },
  {
    eventName: 'contact.updated',
    category: 'contact',
    dtoType: 'identify',
    labelPt: 'Contato atualizado',
    labelEn: 'Contact updated',
    description: 'An existing contact had one or more fields modified.',
    schema: {
      required: { id: f('uuid'), source: f('string') },
      optional: { ...contactIdentityOptionalFields, changes: f('object') },
    },
  },
  {
    eventName: 'contact.deleted',
    category: 'contact',
    dtoType: 'identify',
    labelPt: 'Contato deletado',
    labelEn: 'Contact deleted',
    description: 'A contact record was deleted.',
    schema: {
      required: { source: f('string'), deleted_at: f('date') },
      optional: { reason: f('string') },
    },
  },
  {
    eventName: 'contact.label.added',
    category: 'contact',
    dtoType: 'identify',
    labelPt: 'Etiqueta adicionada ao contato',
    labelEn: 'Label added to contact',
    description: 'A label was applied to a contact.',
    schema: {
      required: { labelName: f('string'), labelId: f('string'), source: f('string') },
      optional: {},
    },
  },
  {
    eventName: 'contact.label.removed',
    category: 'contact',
    dtoType: 'identify',
    labelPt: 'Etiqueta removida do contato',
    labelEn: 'Label removed from contact',
    description: 'A label was removed from a contact.',
    schema: {
      required: { labelName: f('string'), labelId: f('string'), source: f('string') },
      optional: {},
    },
  },
  {
    eventName: 'contact.custom_attribute.changed',
    category: 'contact',
    dtoType: 'identify',
    labelPt: 'Atributo customizado alterado',
    labelEn: 'Custom attribute changed',
    description: 'A custom attribute on a contact was set or changed.',
    schema: {
      required: { attributeName: f('string'), source: f('string') },
      optional: {
        attributeValue: f('string'),
        changeType: f('string'),
        oldValue: f('string'),
      },
    },
  },
  {
    eventName: 'conversation.created',
    category: 'conversation',
    dtoType: 'track',
    labelPt: 'Conversa criada',
    labelEn: 'Conversation created',
    description: 'A new conversation was opened with a contact.',
    schema: {
      required: { conversation_id: f('uuid'), inbox_id: f('uuid'), source: f('string') },
      optional: { inbox_name: f('string'), channel_type: f('string') },
    },
  },
  {
    eventName: 'conversation.resolved',
    category: 'conversation',
    dtoType: 'track',
    labelPt: 'Conversa resolvida',
    labelEn: 'Conversation resolved',
    description: 'An open conversation was closed/resolved.',
    schema: {
      required: { conversation_id: f('uuid'), inbox_id: f('uuid'), source: f('string') },
      optional: {
        inbox_name: f('string'),
        channel_type: f('string'),
        resolved_by_id: f('string'),
        resolved_by_type: f('string'),
        resolution_time_seconds: f('number'),
      },
    },
  },
  {
    eventName: 'conversation.activity',
    category: 'conversation',
    dtoType: 'track',
    labelPt: 'Atividade na conversa',
    labelEn: 'Conversation activity',
    description: 'A lifecycle activity was recorded on a conversation (status change, note, transfer, etc.).',
    schema: {
      required: { conversation_id: f('uuid'), source: f('string') },
      optional: {
        inbox_id: f('uuid'),
        content: f('string'),
        activity_type: f('string'),
      },
    },
  },
  {
    eventName: 'conversation.first_reply',
    category: 'conversation',
    dtoType: 'track',
    labelPt: 'Primeira resposta da conversa',
    labelEn: 'Conversation first reply',
    description: 'The first agent reply was sent on a conversation.',
    schema: {
      required: { conversation_id: f('uuid'), source: f('string') },
      optional: {
        inbox_id: f('uuid'),
        replied_at: f('date'),
        response_time_seconds: f('number'),
      },
    },
  },
  {
    eventName: 'conversation.reply_time',
    category: 'conversation',
    dtoType: 'track',
    labelPt: 'Tempo de resposta da conversa',
    labelEn: 'Conversation reply time',
    description: 'An agent reply was sent; reply-time SLA metric was captured.',
    schema: {
      required: { conversation_id: f('uuid'), source: f('string') },
      optional: {
        inbox_id: f('uuid'),
        reply_time_seconds: f('number'),
        measured_at: f('date'),
      },
    },
  },
  {
    eventName: 'conversation.bot_handoff',
    category: 'conversation',
    dtoType: 'track',
    labelPt: 'Bot transferiu para humano',
    labelEn: 'Bot handed off to human',
    description: 'The bot transferred a conversation to a human agent.',
    schema: {
      required: { conversation_id: f('uuid'), source: f('string') },
      optional: {
        inbox_id: f('uuid'),
        handoff_at: f('date'),
        reason: f('string'),
      },
    },
  },
  {
    eventName: 'conversation.bot_resolved',
    category: 'conversation',
    dtoType: 'track',
    labelPt: 'Conversa resolvida pelo bot',
    labelEn: 'Conversation resolved by bot',
    description: 'The bot resolved the conversation without escalating to a human.',
    schema: {
      required: { conversation_id: f('uuid'), source: f('string') },
      optional: {
        inbox_id: f('uuid'),
        resolved_at: f('date'),
      },
    },
  },
  {
    eventName: 'message.created',
    category: 'message',
    dtoType: 'track',
    labelPt: 'Mensagem criada',
    labelEn: 'Message created',
    description: 'A new incoming or outgoing message was recorded.',
    schema: {
      required: { ...messageCommonRequired, message_type: f('string') },
      optional: { content_type: f('string'), content: f('string') },
    },
  },
  {
    eventName: 'message.delivered',
    category: 'message',
    dtoType: 'track',
    labelPt: 'Mensagem entregue',
    labelEn: 'Message delivered',
    description: 'A message reached the recipient device.',
    schema: {
      required: messageCommonRequired,
      optional: {
        ...messageCommonOptional,
        previous_status: f('string'),
        status: f('string'),
        external_error: f('string'),
      },
    },
  },
  {
    eventName: 'message.read',
    category: 'message',
    dtoType: 'track',
    labelPt: 'Mensagem lida',
    labelEn: 'Message read',
    description: 'A message was read by the recipient.',
    schema: {
      required: messageCommonRequired,
      optional: {
        ...messageCommonOptional,
        previous_status: f('string'),
        status: f('string'),
        external_error: f('string'),
      },
    },
  },
  {
    eventName: 'message.failed',
    category: 'message',
    dtoType: 'track',
    labelPt: 'Mensagem falhou',
    labelEn: 'Message failed',
    description: 'A message delivery attempt failed.',
    schema: {
      required: messageCommonRequired,
      optional: {
        ...messageCommonOptional,
        previous_status: f('string'),
        status: f('string'),
        external_error: f('string'),
      },
    },
  },
  {
    eventName: 'campaign.triggered',
    category: 'campaign',
    dtoType: 'track',
    labelPt: 'Campanha disparada',
    labelEn: 'Campaign triggered',
    description: 'A pipeline-driven campaign entered execution for a contact.',
    schema: {
      required: {
        pipeline_item_id: f('uuid'),
        pipeline_id: f('uuid'),
        source: f('string'),
      },
      optional: {
        conversation_id: f('uuid'),
        contact_id: f('uuid'),
        is_lead: f('boolean'),
        pipeline_name: f('string'),
        pipeline_stage_id: f('uuid'),
        pipeline_stage_name: f('string'),
        assigned_by_id: f('uuid'),
        custom_fields: f('object'),
      },
    },
  },
  {
    eventName: 'campaign.message.sent',
    category: 'campaign',
    dtoType: 'track',
    labelPt: 'Mensagem da campanha enviada',
    labelEn: 'Campaign message sent',
    description: 'A campaign sent a message to a contact.',
    schema: {
      required: { campaign_id: f('uuid'), message_id: f('uuid'), source: f('string') },
      optional: {
        contact_id: f('uuid'),
        channel_type: f('string'),
        template_id: f('uuid'),
      },
    },
  },
  {
    eventName: 'campaign.message.opened',
    category: 'campaign',
    dtoType: 'track',
    labelPt: 'Mensagem da campanha aberta',
    labelEn: 'Campaign message opened',
    description: 'A contact opened a campaign message.',
    schema: {
      required: { campaign_id: f('uuid'), message_id: f('uuid'), source: f('string') },
      optional: { contact_id: f('uuid'), opened_at: f('date') },
    },
  },
  {
    eventName: 'campaign.message.clicked',
    category: 'campaign',
    dtoType: 'track',
    labelPt: 'Link da campanha clicado',
    labelEn: 'Campaign link clicked',
    description: 'A contact clicked a link in a campaign message.',
    schema: {
      required: { campaign_id: f('uuid'), message_id: f('uuid'), source: f('string') },
      optional: {
        contact_id: f('uuid'),
        url: f('string'),
        clicked_at: f('date'),
      },
    },
  },
  {
    eventName: 'pipeline.stage_changed',
    category: 'campaign',
    dtoType: 'track',
    labelPt: 'Stage do pipeline alterado',
    labelEn: 'Pipeline stage changed',
    description: 'A conversation/contact moved between stages in a pipeline.',
    schema: {
      required: {
        pipeline_id: f('uuid'),
        pipeline_stage_id: f('uuid', 'Destination stage'),
        source: f('string'),
      },
      optional: {
        pipeline_name: f('string'),
        pipeline_stage_name: f('string'),
        from_stage_id: f('uuid'),
        from_stage_name: f('string'),
        to_stage_id: f('uuid'),
        to_stage_name: f('string'),
        pipeline_item_id: f('uuid'),
        conversation_id: f('uuid'),
        contact_id: f('uuid'),
      },
    },
  },
  // CRM-316: capture of POST /api/v1/webhooks/purchases/:provider, with the
  // contact the CRM resolved — a journey starts on "bought X", a segment
  // filters "spent more than Y".
  {
    eventName: 'purchase.approved',
    category: 'purchase',
    dtoType: 'track',
    labelPt: 'Compra aprovada',
    labelEn: 'Purchase approved',
    description:
      'A purchase was approved on a payment platform and captured as a lead in the CRM.',
    schema: {
      required: {
        provider: f(
          'string',
          'Payment platform key (virtu, hotmart, kiwify, cakto)',
        ),
        purchase_id: f('string', 'Purchase/order id on the platform'),
        pipeline_id: f('uuid'),
        pipeline_item_id: f('uuid', 'Card that holds the purchase'),
        source: f('string'),
      },
      optional: {
        product: f('string'),
        amount: f(
          'number',
          'Currency major unit (e.g. 197.5 reais), never cents',
        ),
        currency: f('string'),
        platform_event: f('string', 'Event name as the platform sent it'),
        outcome: f('string', 'created | already_in_pipeline'),
        new_contact: f('boolean', 'Whether the purchase created the contact'),
        contact_id: f('uuid'),
        pipeline_name: f('string'),
        pipeline_stage_id: f('uuid'),
        pipeline_stage_name: f('string'),
      },
    },
  },
  {
    eventName: 'scheduled_action.executed',
    category: 'scheduled_action',
    dtoType: 'track',
    labelPt: 'Ação agendada executada',
    labelEn: 'Scheduled action executed',
    description: 'A scheduled action targeting the contact ran successfully.',
    schema: {
      required: scheduledActionRequired,
      optional: {
        ...scheduledActionOptional,
        executed_at: f('date'),
      },
    },
  },
  {
    eventName: 'scheduled_action.failed',
    category: 'scheduled_action',
    dtoType: 'track',
    labelPt: 'Ação agendada falhou',
    labelEn: 'Scheduled action failed',
    description:
      'A scheduled action targeting the contact failed or expired before running.',
    schema: {
      required: scheduledActionRequired,
      optional: {
        ...scheduledActionOptional,
        error_message: f('string', 'Why it failed, truncated to 500 chars'),
        retry_count: f('number', 'Failed attempts so far'),
        will_retry: f('boolean', 'Whether another attempt is scheduled'),
      },
    },
  },
  {
    eventName: 'custom',
    category: 'custom',
    dtoType: 'track',
    labelPt: 'Evento personalizado',
    labelEn: 'Custom event',
    description: 'User-defined event with free-form key/value properties.',
    schema: {
      required: {},
      optional: {},
    },
  },
];

export const EVENT_CATEGORIES: readonly EventCategory[] = [
  'contact',
  'conversation',
  'message',
  'campaign',
  'purchase',
  'scheduled_action',
  'custom',
] as const;

export const EVENT_CATALOG: EventCatalog = Object.freeze(
  ENTRIES.reduce<EventCatalog>((acc, entry) => {
    acc[entry.eventName] = entry;
    return acc;
  }, {} as EventCatalog),
);

export function assertCatalogCoversAllNames(): void {
  const missing = EVENT_NAMES.filter((n: EvoFlowEventName) => !EVENT_CATALOG[n]);
  if (missing.length > 0) {
    throw new Error(`Event catalog missing entries: ${missing.join(', ')}`);
  }
}
