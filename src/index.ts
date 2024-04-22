/**
 * Welcome to Cloudflare Workers!
 *
 * This is a template for a Queue consumer: a Worker that can consume from a
 * Queue: https://developers.cloudflare.com/queues/get-started/
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
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
   * - Enable support for auto caption jobs
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

  /**
   * QUEUE CONSUMER HANDLER
   *
   * Need to:
   * - Figure out how to make this a PULL handler
   * - If Stream sends a 429, bail out and retry the entire batch later
   * - Report failures
   *
   * @param batch
   * @param env
   */
  async queue(batch: MessageBatch, env: Env): Promise<void> {
    for (let message of batch.messages) {
      await processMessage(message, env);
    }
  },
};
