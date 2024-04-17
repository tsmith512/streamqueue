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

export interface Env {
  VIDQUEUE: Queue;
}

type uploadOps = "uploadFetch";
type followupOps = "enableMP4Download" | "enableAutoCaptionsEN";

interface StreamQueueMessage {
  action: uploadOps | followupOps;
  notes: string[]; // For testing and debugging notes
}

interface UploadRequestMessage extends StreamQueueMessage {
  action: uploadOps;
  name: string;
  creator: string;
  source: string;
}

interface SecondaryOpRequestMessage extends StreamQueueMessage {
  action: followupOps;
  uid: string;
}

export default {
  /**
   * FETCH HANDLER
   *
   * Need to:
   * - Accept messages directly somehow
   * - Accept messages from Stream VOD webhooks
   *   - And validate them...
   * - Add messages to the VIDQUEUE queue with an action and necessary data:
   *   - Video name
   *   - Creator ID
   *   - Next actions (mp4download, ???)
   * - Accept and enqueue requests to fetch from URL
   * - Accept and enqueue requests to enable MP4 downloads for a video
   *
   * @param req (Request) inbound request object
   * @param env (Environemnt) contains env vars and Workers bindings
   * @param ctx
   * @returns (Response)
   */
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    await env.VIDQUEUE.send({
      url: req.url,
      method: req.method,
      headers: Object.fromEntries(req.headers),
    });
    return new Response('Sent message to the queue');
  },

  /**
   * QUEUE CONSUMER HANDLER
   *
   * Need to:
   * - Figure out how to make this a PULL handler
   * - Accept a batch of messages
   * - Process requests (uploadfetch, mp4download, ???)
   * - If Stream sends a 429, bail out and retry the entire batch later
   * - (Debugging) Report inbound messages
   * - (Debugging) Report successes
   * - Report failures
   *
   * @param batch
   * @param env
   */
  async queue(batch: MessageBatch<Error>, env: Env): Promise<void> {
    // A queue consumer can make requests to other endpoints on the Internet,
    // write to R2 object storage, query a D1 Database, and much more.
    for (let message of batch.messages) {
      // Process each message (we'll just log these)
      console.log(`message ${message.id} processed: ${JSON.stringify(message.body)}`);
    }
  },
};
