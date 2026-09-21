import { EvoExtensionPoints } from '../../evo-extension-points/registry';
import {
  BrokerHealth,
  BrokerMessage,
  IMessageBroker,
} from './interfaces/message-broker.interface';

/**
 * Applies the `outbound_headers` and `inbound_message_context` extension points
 * around whichever adapter `BROKER_TYPE` selected, so neither adapter nor any
 * consumer has to know about them.
 */
export class ContextPropagatingBroker implements IMessageBroker {
  constructor(readonly adapter: IMessageBroker) {}

  publish<T>(
    topic: string,
    payload: T,
    headers: Record<string, string> = {},
  ): Promise<void> {
    const outbound = EvoExtensionPoints.get('outbound_headers')();
    return this.adapter.publish(topic, payload, { ...outbound, ...headers });
  }

  subscribe<T>(
    topic: string,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void> {
    return this.adapter.subscribe(topic, this.withInboundContext(handler));
  }

  subscribePattern<T>(
    prefix: string,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void> {
    return this.adapter.subscribePattern(
      prefix,
      this.withInboundContext(handler),
    );
  }

  ack(msg: BrokerMessage): Promise<void> {
    return this.adapter.ack(msg);
  }

  nack(msg: BrokerMessage, requeue?: boolean): Promise<void> {
    return this.adapter.nack(msg, requeue);
  }

  provisionTopic(topic: string): Promise<void> {
    return this.adapter.provisionTopic(topic);
  }

  getTopicLag(topic: string): Promise<number> {
    return this.adapter.getTopicLag(topic);
  }

  healthCheck(expectedTopics: string[]): Promise<BrokerHealth> {
    return this.adapter.healthCheck(expectedTopics);
  }

  private withInboundContext<T>(
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): (msg: BrokerMessage<T>) => Promise<void> {
    return (msg) =>
      EvoExtensionPoints.get('inbound_message_context')(msg.headers, () =>
        handler(msg),
      );
  }
}
