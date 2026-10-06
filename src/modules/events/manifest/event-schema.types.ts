import type { EvoFlowEventName } from '../event-names.enum';

export type FieldType = 'string' | 'number' | 'boolean' | 'date' | 'uuid' | 'object';

export interface FieldSpec {
  type: FieldType;
  description?: string;
}

export interface EventSchema {
  required: Record<string, FieldSpec>;
  optional: Record<string, FieldSpec>;
}

export type EventCategory =
  | 'contact'
  | 'conversation'
  | 'message'
  | 'campaign'
  | 'purchase'
  | 'scheduled_action'
  | 'custom';

export type EventDtoType = 'track' | 'identify';

export interface EventCatalogEntry {
  eventName: EvoFlowEventName | 'custom';
  category: EventCategory;
  // Which evo-flow DTO this event lands on. contact.* travel through
  // /events/identify (ContactEventsListener#IDENTIFY_PATH); every other
  // canonical event uses /events/track.
  dtoType: EventDtoType;
  labelPt: string;
  labelEn: string;
  description: string;
  schema: EventSchema;
}

export type EventCatalog = Record<EvoFlowEventName | 'custom', EventCatalogEntry>;
