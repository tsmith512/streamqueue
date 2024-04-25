/**
 * @file queueing.ts
 *
 * Functions that handle reading/processing queue messages.
 */

import { Env, SecondaryOpRequestMessage, UploadRequestMessage } from ".";
import { enableAutoCaptions, enableMP4Download, uploadFetch } from "./outbound";

export const processMessage = async (message: Message, env: Env): Promise<number> => {
  const payload = message.body as UploadRequestMessage | SecondaryOpRequestMessage;
  console.log(`Reviewing message ${message.id}: ${JSON.stringify(payload)}`);

  // Let's decide to ack or retry later...
  let code = -1;

  switch (payload.action) {
    case 'uploadFetch':
      console.log(`Received upload fetch request for ${payload.source}.`);
      code = await uploadFetch(payload, env);
      break;
    case 'enableMP4Download':
      console.log(`Received MP4 Download generation request for ${payload.uid}.`);
      code = await enableMP4Download(payload, env);
      break;
    case 'enableAutoCaptionsEN':
      console.log(`Received auto-generated captions request for ${payload.uid}.`);
      code = await enableAutoCaptions(payload, env);
      break;
    default:
      console.log(`Unknown action requested: ${payload}`);
      code = 400;
      break;
  }

  if (retry(code)) {
    message.retry({ delaySeconds: 60 });
  } else {
    message.ack();
  }

  return code;
}

/**
 * Lots of reasons things may succeed for fail. Based on a code, decide to retry
 * a message or not.
 *
 * @param code (number) Response Code from Stream API
 * @returns (boolean) Should we retry this request later?
 */
const retry = (code: number): boolean => {
  if (code >= 200 && code < 300) {
    console.log(`Done.`);
    return false;
  }

  switch (code) {
    case 429:
      console.log(`Rate limited. Will retry.`);
      return true;
      break;
    case 409:
      console.log(`Conflict. Most likely a duplicate request. Will not retry.`);
      return false;
      break;
    case 400:
      console.log(`Bad request. Will not retry.`);
      return false;
      break;
    default:
      console.log(`Unanticipated code ${code}, will retry.`);
      return true;
  }
}
