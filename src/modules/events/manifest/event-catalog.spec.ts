import { EVENT_NAMES } from '../event-names.enum';
import {
  getEventCatalog,
  getEvent,
  getEventsByCategory,
  isCanonicalEvent,
  EVENT_CATEGORIES,
} from './index';

describe('events manifest catalog', () => {
  it('exposes one entry per EVENT_NAME (including the custom sentinel)', () => {
    const catalog = getEventCatalog();
    const names = catalog.map((e) => e.eventName);
    for (const n of EVENT_NAMES) {
      expect(names).toContain(n);
    }
    expect(names).toContain('custom');
    expect(catalog.length).toBe(EVENT_NAMES.length);
  });

  it('returns a known entry for getEvent(canonical name)', () => {
    const entry = getEvent('message.delivered');
    expect(entry).toBeDefined();
    expect(entry?.category).toBe('message');
    expect(entry?.schema.required).toHaveProperty('message_id');
    expect(entry?.schema.required).toHaveProperty('channel_type');
  });

  // L4: invariant — custom MUST always accept any payload. No required, no
  // optional. AC4 ("custom accepts any key/value") depends on this.
  it('returns the custom entry with empty schema (AC4 invariant)', () => {
    const custom = getEvent('custom');
    expect(custom).toBeDefined();
    expect(custom?.schema.required).toEqual({});
    expect(custom?.schema.optional).toEqual({});
  });

  // CRM-316: the purchase captured by the CRM webhook is a first-class event,
  // in its own category, with the fields a journey trigger / segment filters on.
  it('exposes purchase.approved as a track event in the purchase category', () => {
    const entry = getEvent('purchase.approved');
    expect(entry).toBeDefined();
    expect(entry?.category).toBe('purchase');
    expect(entry?.dtoType).toBe('track');
    expect(Object.keys(entry!.schema.required).sort()).toEqual([
      'pipeline_id',
      'pipeline_item_id',
      'provider',
      'purchase_id',
      'source',
    ]);
    expect(entry?.schema.optional.amount.type).toBe('number');
    expect(entry?.schema.optional.product.type).toBe('string');
    expect(EVENT_CATEGORIES).toContain('purchase');
    expect(getEventsByCategory('purchase').map((e) => e.eventName)).toEqual([
      'purchase.approved',
    ]);
  });

  it('exposes the scheduled action outcomes as track events in their own category', () => {
    const executed = getEvent('scheduled_action.executed');
    const failed = getEvent('scheduled_action.failed');
    for (const entry of [executed, failed]) {
      expect(entry?.category).toBe('scheduled_action');
      expect(entry?.dtoType).toBe('track');
      expect(Object.keys(entry!.schema.required).sort()).toEqual([
        'action_type',
        'scheduled_action_id',
        'scheduled_for',
        'source',
      ]);
    }
    expect(failed?.schema.optional.error_message.type).toBe('string');
    expect(failed?.schema.optional.will_retry.type).toBe('boolean');
    expect(
      getEventsByCategory('scheduled_action').map((e) => e.eventName),
    ).toEqual(['scheduled_action.executed', 'scheduled_action.failed']);
  });

  it('returns undefined for an unknown event name', () => {
    expect(getEvent('not.a.real.event')).toBeUndefined();
  });

  it('identifies events declared in EVENT_NAMES via isCanonicalEvent', () => {
    expect(isCanonicalEvent('contact.created')).toBe(true);
    expect(isCanonicalEvent('custom')).toBe(true);
    expect(isCanonicalEvent('not.a.real.event')).toBe(false);
  });

  it('groups events by category', () => {
    const byCategory = Object.fromEntries(
      EVENT_CATEGORIES.map((c) => [c, getEventsByCategory(c).map((e) => e.eventName)]),
    );
    expect(byCategory.contact).toEqual(
      expect.arrayContaining([
        'contact.created',
        'contact.updated',
        'contact.deleted',
        'contact.label.added',
        'contact.label.removed',
        'contact.custom_attribute.changed',
      ]),
    );
    expect(byCategory.conversation).toEqual(
      expect.arrayContaining(['conversation.created', 'conversation.resolved']),
    );
    expect(byCategory.message).toEqual(
      expect.arrayContaining(['message.created', 'message.delivered', 'message.read', 'message.failed']),
    );
    expect(byCategory.campaign).toEqual(
      expect.arrayContaining([
        'campaign.triggered',
        'campaign.message.sent',
        'campaign.message.opened',
        'campaign.message.clicked',
      ]),
    );
    expect(byCategory.custom).toEqual(['custom']);
  });

  it('declares labelPt and labelEn for every entry', () => {
    for (const entry of getEventCatalog()) {
      expect(entry.labelPt.length).toBeGreaterThan(0);
      expect(entry.labelEn.length).toBeGreaterThan(0);
      expect(entry.description.length).toBeGreaterThan(0);
    }
  });
});
