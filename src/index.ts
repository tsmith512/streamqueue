/**
 * VIDQUEUE is a sample Worker that leverages Cloudflare Workers Queues to
 * to churn through large backlogs for Cloudflare Stream.
 */

import { AutoRouter } from 'itty-router';
import { processInboundWebhook, requestStreamFetch } from './inbound';
import { processMessage } from './queueing';

export interface Env {
  // This binding is set in wrangler.toml
  VIDQUEUE: Queue;

  // This is an ENV var in wrangler.toml
  CF_API: string;

  // Put these as secrets
  CF_STREAM_KEY: string;
  CF_ACCT_TAG: string;
  CF_QUEUE_ID: string; // @TODO: Can I get this from the binding? It's VIDQUEUE's ID.
}

type uploadOps = "uploadFetch";
type followupOps = "enableMP4Download" | "enableAutoCaptionsEN";

interface StreamQueueMessage {
  action: uploadOps | followupOps;
  notes: string[]; // For testing and debugging notes
}

export interface UploadRequestMessage extends StreamQueueMessage {
  action: uploadOps;
  name: string;
  creator: string;
  source: string;
}

export interface SecondaryOpRequestMessage extends StreamQueueMessage {
  action: followupOps;
  uid: string;
}

export default {
  /**
   * FETCH HANDLER
   *
   * Need to:
   * - Authenticate direct messages
   * - Validate Stream webhooks
   * - Some kind of way to determine what videos get what jobs?
   *
   * @param req (Request) inbound request object
   * @param env (Environemnt) contains env vars and Workers bindings
   * @param ctx
   * @returns (Response)
   */
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const router = AutoRouter();
    router
      .get('/api', () => `Hello`)
      .post('/api/fetch', requestStreamFetch)
      .post('/inbound', processInboundWebhook) // @TODO: Move this
    ;

    return await router.fetch(req, env, ctx);
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const endpoint = `${env.CF_API}/${env.CF_ACCT_TAG}/queues/${env.CF_QUEUE_ID}/messages`;
    console.log(endpoint);
    const response = await fetch(`${endpoint}/pull`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'authorization': `Bearer ${env.CF_STREAM_KEY}`,
      },
      body: JSON.stringify({
        // @TODO: visibility_timeout and batch_size are both defined in
        // wrangler.toml but I don't know if that's associated to the worker,
        // do they have to be set here?
      }),
    });

    if (response.ok) {
      // @TODO some type safety?
      const payload: any = await response.json();

      const acks: string[] = [];
      const retries: string[] = [];

      if (payload?.result?.messages?.length) {
        for (let message of payload.result.messages) {
          // A pulled `message` vs a standard push consumer's Message are the
          // same except the retry and ack methods. Add those callbacks to add
          // the message lease_id to the corresponding list.
          message.retry = (options?: QueueRetryOptions) => {
            retries.push(message.lease_id);
          };
          message.ack = () => {
            acks.push(message.lease_id);
          };

          const code = await processMessage(message, env);
          // @TODO: Bail and retry the rest on 429? Queues docs say messages not
          // reported on will be redelivered.
        }
      }

      console.log(`Ran batch. Retry: ${retries.join(',')}. Acknowledge: ${acks.join(',')}.`);

      const report = JSON.stringify({
        acks: acks.map(id => ({ lease_id: `${id}` })),
        retries: retries.map(id => ({
          lease_id: `${id}`,
          delay_seconds: 60 * 5,
        }))
      });

      const reply = await fetch(`${endpoint}/ack`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'authorization': `Bearer $${env.CF_STREAM_KEY}`,
        },
        body: report,
      });

      console.log(`Ack response ${reply.status} ${reply.statusText}. Reported ${report}.`);

    } else {
      console.log(`Abort. Got bad response from Queues API: ${response.status} ${response.statusText}`);
    }
  },

  /**
   * QUEUE CONSUMER HANDLER
   *
   * Need to:
   * - Figure out how to make this a PULL handler
   * - If Stream sends a 429, bail out and retry the entire batch later
   *
   * @param batch
   * @param env
   */
  // async queue(batch: MessageBatch, env: Env): Promise<void> {
  //   for (let message of batch.messages) {
  //     const code = await processMessage(message, env);
  //     if (code === 429) {
  //       // We got rate limited.
  //       // Any message already acknowledged will be marked as successful, so
  //       // mark the rest of the batch to try again in 5 minutes.
  //       batch.retryAll({ delaySeconds: 60 * 5 });
  //       break;
  //     }
  //   }
  // },
};
