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

import { AutoRouter } from 'itty-router' // ~1kB
export interface Env {
  VIDQUEUE: Queue;
  CF_STREAM_KEY: string;
  CF_ACCT_TAG: string;
  CF_API: string;
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
  async queue(batch: MessageBatch, env: Env): Promise<void> {
    for (let message of batch.messages) {
      const payload = message.body as UploadRequestMessage | SecondaryOpRequestMessage;
      console.log(`Reviewing message ${message.id}: ${JSON.stringify(payload)}`);

      // Let's decide to ack or retry later...
      let success = false;

      switch (payload.action) {
        case 'uploadFetch':
          console.log(`Received upload fetch request for ${payload.source}.`);

          const res = await fetch(`${env.CF_API}/${env.CF_ACCT_TAG}/stream/copy`, {
            headers: {
              'Authorization': `Bearer ${env.CF_STREAM_KEY}`,
            },
            method: 'POST',
            body: JSON.stringify({
              creator: payload.creator,
              meta: {
                name: payload.name,
              },
              url: payload.source,
            }),
          });

          console.log(`Stream responded ${res.status} ${res.statusText}: \n${JSON.stringify(await res.json())}`);

          // @TODO: There are lots of reasons this may fail...
          success = res.ok
          break;
        case 'enableMP4Download':
          console.log(`Received MP4 Download generation request for ${payload.uid}.`);

          const dlRes = await fetch(`${env.CF_API}/${env.CF_ACCT_TAG}/stream/${payload.uid}/downloads`, {
            headers: {
              'Authorization': `Bearer ${env.CF_STREAM_KEY}`,
            },
            method: 'POST',
          });

          console.log(`Stream responded ${dlRes.status} ${dlRes.statusText}: \n${JSON.stringify(await dlRes.json())}`);

          success = dlRes.ok;
          break;
        case 'enableAutoCaptionsEN':
          console.log('Enabling auto captions is not yet supported.');
          success = true;
          break;
      }

      if (success) {
        message.ack();
      } else {
        // Delay for 1 minutes and try again...
        message.retry({ delaySeconds: 60 });
      }
    }
  },
};

/**
 * Enqueue a request to trigger a fetch-from-URL
 * @param req
 * @param env
 * @param ctx
 * @returns
 */
const requestStreamFetch = async (req: Request, env: Env, ctx: ExecutionContext): Promise<Response> => {
  // @TODO: Expect a message we can enqueue directly.
  const payload = await req.json() as UploadRequestMessage;

  // @TODO: Some kind of validation or authoriation

  // @TODO: Build a message we can enqueue, but for initial test, see above
  // const message: UploadRequestMessage = {};
  const message: UploadRequestMessage = {
    action: 'uploadFetch',
    name: payload.name || 'untitled',
    creator: payload.creator || 'vidqueue',
    source: payload.source,
    notes: [`Fetch request received and enqueued at ${new Date()}`],
  };

  await env.VIDQUEUE.send(message);

  return new Response(JSON.stringify({
    status: 'Enqueued',
    message
  }), {
    status: 201
  });
};

const processInboundWebhook = async (req: Request, env: Env, ctx: ExecutionContext): Promise<Response> => {
  // @TODO: Do we type annotate Stream inbound webhooks or just yolo it?
  const payload: any = await req.json();

  console.log(req);
  console.log(payload);

  // Stream sends a webbook when a video is ready for playback or errored.
  if (payload?.status?.state !== 'ready') {
    // @TODO: Report video encoding failure.

    // Webhook sender doesn't care, close out with an acknowledgement and be done.
    return new Response(null, { status: 204 });
  }

  // @TODO: For now, assume we want to make an MP4 Download for everything we get
  const message: SecondaryOpRequestMessage = {
    action: 'enableMP4Download',
    uid: payload.uid,
    notes: [`Generated from inbound webhook and enqueued at ${new Date()}`],
  };

  await env.VIDQUEUE.send(message);

  // Webhook sender doesn't care, close out with an acknowledgement and be done.
  return new Response(null, { status: 204 });
};
