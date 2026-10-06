import { BadRequestException } from '@nestjs/common';
import { EventSchemaValidationPipe } from './event-schema-validation.pipe';

const bodyMetadata = { type: 'body' as const, metatype: undefined, data: undefined };

describe('EventSchemaValidationPipe', () => {
  const pipe = new EventSchemaValidationPipe();

  it('passes through when not a body argument', () => {
    expect(pipe.transform({ event: 'message.delivered' }, { type: 'query', metatype: undefined, data: undefined } as any))
      .toEqual({ event: 'message.delivered' });
  });

  it('passes through when event/eventName field is missing', () => {
    const value = { messageId: 'm1', contactId: '42' };
    expect(pipe.transform(value, bodyMetadata)).toBe(value);
  });

  it('passes through when event name is not in the catalog (delegate to @IsIn)', () => {
    const value = { event: 'not.a.real.event', properties: {} };
    expect(pipe.transform(value, bodyMetadata)).toBe(value);
  });

  // CRM-316: the payload the CRM emits for an approved purchase, as-is.
  describe('purchase.approved (CRM-316)', () => {
    const purchase = {
      messageId: 'purchase.approved:c1:virtu.ord-1',
      contactId: '550e8400-e29b-41d4-a716-446655440001',
      event: 'purchase.approved',
      properties: {
        provider: 'virtu',
        purchase_id: 'ord-1',
        pipeline_id: '550e8400-e29b-41d4-a716-446655440002',
        pipeline_item_id: '550e8400-e29b-41d4-a716-446655440003',
        source: 'purchase_webhook',
        product: 'Curso X',
        amount: 297.9,
        currency: 'BRL',
        outcome: 'created',
        new_contact: true,
      },
    };

    it('accepts the CRM payload with a numeric amount', () => {
      expect(pipe.transform(purchase, bodyMetadata)).toBe(purchase);
    });

    it('rejects a purchase without purchase_id', () => {
      const rest: Record<string, unknown> = { ...purchase.properties };
      delete rest.purchase_id;
      expect(() =>
        pipe.transform({ ...purchase, properties: rest }, bodyMetadata),
      ).toThrow(BadRequestException);
    });

    it('rejects a string amount (the CRM must send a number)', () => {
      const value = {
        ...purchase,
        properties: { ...purchase.properties, amount: '297.90' },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(
        BadRequestException,
      );
    });
  });

  describe('scheduled_action.failed', () => {
    const failed = {
      messageId: 'sa-1-failed-1',
      contactId: '550e8400-e29b-41d4-a716-446655440001',
      event: 'scheduled_action.failed',
      properties: {
        scheduled_action_id: 42,
        action_type: 'execute_webhook',
        scheduled_for: '2026-10-06T14:00:00Z',
        source: 'scheduled_action',
        error_message: 'Webhook failed with status 500',
        retry_count: 1,
        will_retry: true,
      },
    };

    it('accepts the CRM payload with a numeric action id', () => {
      expect(pipe.transform(failed, bodyMetadata)).toBe(failed);
    });

    it('rejects a payload without scheduled_for', () => {
      const rest: Record<string, unknown> = { ...failed.properties };
      delete rest.scheduled_for;
      expect(() =>
        pipe.transform({ ...failed, properties: rest }, bodyMetadata),
      ).toThrow(BadRequestException);
    });
  });

  describe('AC3 — required field validation (track path)', () => {
    it('rejects message.delivered without message_id with MissingRequiredField', () => {
      const value = {
        messageId: 'm1',
        contactId: '42',
        event: 'message.delivered',
        properties: { channel_type: 'Channel::Whatsapp', conversation_id: '550e8400-e29b-41d4-a716-446655440002', source: 'messaging' },
      };

      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
      try {
        pipe.transform(value, bodyMetadata);
      } catch (err) {
        expect((err as BadRequestException).getResponse()).toEqual({
          error: 'MissingRequiredField',
          field: 'message_id',
          eventName: 'message.delivered',
        });
      }
    });

    it('accepts message.delivered with all required fields present', () => {
      const value = {
        messageId: 'm1',
        contactId: '42',
        event: 'message.delivered',
        properties: {
          message_id: '550e8400-e29b-41d4-a716-446655440000',
          channel_type: 'Channel::Whatsapp',
          conversation_id: '550e8400-e29b-41d4-a716-446655440001',
          source: 'messaging',
        },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });
  });

  describe('AC3 — required field validation (identify path)', () => {
    it('rejects contact.created without id when traits is the payload carrier', () => {
      const value = {
        messageId: 'm1',
        contactId: '42',
        eventName: 'contact.created',
        traits: { source: 'contact_created' },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
      try {
        pipe.transform(value, bodyMetadata);
      } catch (err) {
        expect((err as BadRequestException).getResponse()).toEqual({
          error: 'MissingRequiredField',
          field: 'id',
          eventName: 'contact.created',
        });
      }
    });

    it('accepts contact.created when id and source are present in traits', () => {
      const value = {
        messageId: 'm1',
        contactId: '42',
        eventName: 'contact.created',
        traits: { id: '550e8400-e29b-41d4-a716-446655440000', source: 'contact_created' },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });
  });

  describe('AC4 — custom event accepts free key/value', () => {
    it('accepts any payload shape when event=custom', () => {
      const value = {
        messageId: 'm1',
        contactId: '42',
        event: 'custom',
        properties: { whatever: 'value', deeply: 123, nested: true },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });

    it('accepts empty properties when event=custom', () => {
      const value = { messageId: 'm1', contactId: '42', event: 'custom', properties: {} };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });
  });

  describe('H1: empty string treated as missing for string-like required fields', () => {
    it('rejects empty-string message_id as MissingRequiredField', () => {
      const value = {
        messageId: 'm1',
        contactId: '42',
        event: 'message.delivered',
        properties: {
          message_id: '',
          channel_type: 'Channel::Whatsapp',
          conversation_id: '550e8400-e29b-41d4-a716-446655440002',
          source: 'messaging',
        },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
      try {
        pipe.transform(value, bodyMetadata);
      } catch (err) {
        expect((err as BadRequestException).getResponse()).toEqual({
          error: 'MissingRequiredField',
          field: 'message_id',
          eventName: 'message.delivered',
        });
      }
    });

    it('still accepts false/0 for boolean/number-typed fields (not treated as missing)', () => {
      const value = {
        messageId: 'm1',
        event: 'campaign.triggered',
        properties: {
          pipeline_item_id: '550e8400-e29b-41d4-a716-446655440000',
          pipeline_id: '550e8400-e29b-41d4-a716-446655440001',
          source: 's',
          is_lead: false,
          assigned_by_id: 0,
        },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });
  });

  describe('M1: uuid type strictness', () => {
    it('rejects arbitrary non-UUID non-numeric strings for :uuid fields', () => {
      const value = {
        messageId: 'm1',
        event: 'message.delivered',
        properties: {
          message_id: 'not-a-uuid',
          channel_type: 'Channel::Whatsapp',
          conversation_id: 'also-not-a-uuid',
          source: 's',
        },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
    });

    it('accepts canonical UUID strings', () => {
      const value = {
        messageId: 'm1',
        event: 'message.delivered',
        properties: {
          message_id: '550e8400-e29b-41d4-a716-446655440000',
          channel_type: 'Channel::Whatsapp',
          conversation_id: '550e8400-e29b-41d4-a716-446655440001',
          source: 's',
        },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });

    it('accepts numeric strings (legacy contact_id paths emit "42")', () => {
      const value = {
        messageId: 'm1',
        event: 'message.delivered',
        properties: {
          message_id: '42',
          channel_type: 'Channel::Whatsapp',
          conversation_id: '99',
          source: 's',
        },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });

    it('rejects float strings (Ruby Integer() does not accept these either)', () => {
      const value = {
        messageId: 'm1',
        event: 'message.delivered',
        properties: {
          message_id: '12.5',
          channel_type: 'Channel::Whatsapp',
          conversation_id: '99',
          source: 's',
        },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
    });

    it('rejects non-integer numbers (TS/Ruby symmetry)', () => {
      const value = {
        messageId: 'm1',
        event: 'message.delivered',
        properties: {
          message_id: 12.5,
          channel_type: 'Channel::Whatsapp',
          conversation_id: '99',
          source: 's',
        },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
    });

    it('rejects "Infinity" string', () => {
      const value = {
        messageId: 'm1',
        event: 'message.delivered',
        properties: {
          message_id: 'Infinity',
          channel_type: 'Channel::Whatsapp',
          conversation_id: '99',
          source: 's',
        },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
    });

    it('accepts raw numbers (legacy integer ids)', () => {
      const value = {
        messageId: 'm1',
        event: 'message.delivered',
        properties: {
          message_id: 42,
          channel_type: 'Channel::Whatsapp',
          conversation_id: 99,
          source: 's',
        },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });
  });

  describe('type validation', () => {
    it('rejects InvalidFieldType when message_id is a boolean (uuid type expected)', () => {
      const value = {
        messageId: 'm1',
        contactId: '42',
        event: 'message.delivered',
        properties: {
          message_id: true,
          channel_type: 'Channel::Whatsapp',
          conversation_id: '550e8400-e29b-41d4-a716-446655440001',
          source: 'messaging',
        },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
      try {
        pipe.transform(value, bodyMetadata);
      } catch (err) {
        expect((err as BadRequestException).getResponse()).toMatchObject({
          error: 'InvalidFieldType',
          field: 'message_id',
          eventName: 'message.delivered',
          expected: 'uuid',
        });
      }
    });

    it('accepts ISO-8601 strings for date fields', () => {
      const value = {
        messageId: 'm1',
        eventName: 'contact.deleted',
        traits: { source: 'contact_deleted', deleted_at: '2026-05-25T12:00:00.000Z' },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });

    it('rejects date field when value is not parseable', () => {
      const value = {
        messageId: 'm1',
        eventName: 'contact.deleted',
        traits: { source: 'contact_deleted', deleted_at: 'not-a-date' },
      };
      expect(() => pipe.transform(value, bodyMetadata)).toThrow(BadRequestException);
    });
  });

  describe('payload carrier selection', () => {
    it('uses properties for track events when both properties and traits are present', () => {
      const value = {
        messageId: 'm1',
        event: 'conversation.created',
        properties: { conversation_id: '550e8400-e29b-41d4-a716-446655440002', inbox_id: '550e8400-e29b-41d4-a716-446655440007', source: 'conversation_management' },
        traits: { source: 'unrelated' },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });

    it('falls back to traits when properties is absent', () => {
      const value = {
        messageId: 'm1',
        eventName: 'contact.label.added',
        traits: { labelName: 'hot', labelId: 'lbl-1', source: 'label_added' },
      };
      expect(pipe.transform(value, bodyMetadata)).toBe(value);
    });
  });
});
