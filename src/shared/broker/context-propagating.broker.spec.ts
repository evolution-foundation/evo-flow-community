import { EvoExtensionPoints } from '../../evo-extension-points';
import { ContextPropagatingBroker } from './context-propagating.broker';
import {
  BrokerMessage,
  IMessageBroker,
} from './interfaces/message-broker.interface';

type Handler = (msg: BrokerMessage) => Promise<void>;

function fakeAdapter() {
  const handlers: Handler[] = [];
  const register = (_topic: string, handler: Handler) => {
    handlers.push(handler);
    return Promise.resolve();
  };
  const adapter = {
    publish: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn(register),
    subscribePattern: jest.fn(register),
  } as unknown as IMessageBroker & { publish: jest.Mock };
  return { adapter, handlers };
}

const message: BrokerMessage = {
  id: 'm-1',
  payload: {},
  headers: { 'x-ctx': 'abc' },
  raw: null,
};

describe('ContextPropagatingBroker', () => {
  afterEach(() => EvoExtensionPoints.reset());

  it('publishes with no extra headers by default', async () => {
    const { adapter } = fakeAdapter();

    await new ContextPropagatingBroker(adapter).publish('t', { a: 1 });

    expect(adapter.publish).toHaveBeenCalledWith('t', { a: 1 }, {});
  });

  it('stamps outbound headers on publish, letting the caller win on a clash', async () => {
    EvoExtensionPoints.replace('outbound_headers', () => ({
      'x-ctx': 'from-overlay',
      'x-other': 'kept',
    }));
    const { adapter } = fakeAdapter();

    await new ContextPropagatingBroker(adapter).publish(
      't',
      {},
      { 'x-ctx': 'from-caller' },
    );

    expect(adapter.publish).toHaveBeenCalledWith(
      't',
      {},
      { 'x-ctx': 'from-caller', 'x-other': 'kept' },
    );
  });

  it.each(['subscribe', 'subscribePattern'] as const)(
    '%s runs the handler inside the inbound context with the message headers',
    async (method) => {
      const seen: string[] = [];
      EvoExtensionPoints.replace(
        'inbound_message_context',
        async (headers, work) => {
          seen.push(`enter:${headers['x-ctx']}`);
          const result = await work();
          seen.push('exit');
          return result;
        },
      );
      const { adapter, handlers } = fakeAdapter();
      const handler = jest.fn(() => {
        seen.push('handler');
        return Promise.resolve();
      });

      await new ContextPropagatingBroker(adapter)[method]('t', handler);
      await handlers[0](message);

      expect(handler).toHaveBeenCalledWith(message);
      expect(seen).toEqual(['enter:abc', 'handler', 'exit']);
    },
  );
});
