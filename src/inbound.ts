/**
 * @file inbound.ts
 *
 * Functions that handle requests we're getting from external sources, either
 * Stream Webhooks or direct API requests of this service.
 */

import {
  Env,
  UploadRequestMessage,
  SecondaryOpRequestMessage,
} from '.';

/**
 * Route handler to enqueue a request to trigger a fetch-from-URL
 *
 * @param req
 * @param env
 * @param ctx
 * @returns
 */
export const requestStreamFetch = async (req: Request, env: Env, ctx: ExecutionContext): Promise<Response> => {
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

/**
 * Route handler to process a Stream VOD webhook. For now, we'll assume any
 * video ready-to-stream should get an MP4 Download and English Subtitles
 * generated. These will be enqueued as separate jobs.
 *
 * @param req
 * @param env
 * @param ctx
 * @returns
 */
export const processInboundWebhook = async (req: Request, env: Env, ctx: ExecutionContext): Promise<Response> => {
  // @TODO: Do we type annotate Stream inbound webhooks or just yolo it?
  const payload: any = await req.json();

  console.log(req);
  console.log(payload);

  // Stream sends a webbook when a video is ready for playback or errored.
  if (payload?.status?.state !== 'ready') {
    // @TODO: This will be a description of an error. Do sometihng better with it,
    // like maybe punt it to the DLQ.
    console.log(payload?.status);

    // Webhook sender doesn't care, close out with an acknowledgement.
    return new Response(null, { status: 204 });
  }

  // @TODO: For now, assume we want to make an MP4 Download for everything we get
  const requestMP4: SecondaryOpRequestMessage = {
    action: 'enableMP4Download',
    uid: payload.uid,
    notes: [`Generated from inbound webhook and enqueued at ${new Date()}`],
  };

  await env.VIDQUEUE.send(requestMP4);

  const requestCaptions: SecondaryOpRequestMessage = {
    action: 'enableAutoCaptionsEN',
    uid: payload.uid,
    notes: [`Generated from inbound webhook and enqueued at ${new Date()}`],
  };

  await env.VIDQUEUE.send(requestCaptions);

  // Webhook sender doesn't care, close out with an acknowledgement and be done.
  return new Response(null, { status: 204 });
};
