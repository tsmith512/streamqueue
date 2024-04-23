/**
 * @file queueing.ts
 *
 * Functions that handle reading/processing queue messages.
 */

import { Env, SecondaryOpRequestMessage, UploadRequestMessage } from ".";
import { enableAutoCaptions, enableMP4Download, uploadFetch } from "./outbound";

export const processMessage = async (message: Message, env: Env): Promise<void> => {
  const payload = message.body as UploadRequestMessage | SecondaryOpRequestMessage;
  console.log(`Reviewing message ${message.id}: ${JSON.stringify(payload)}`);

  // Let's decide to ack or retry later...
  let success = false;

  switch (payload.action) {
    case 'uploadFetch':
      console.log(`Received upload fetch request for ${payload.source}.`);

      const response = await uploadFetch(payload, env);
      // @TODO: There are lots of reasons this may fail...
      success = response >= 200 && response < 300;
      break;
    case 'enableMP4Download':
      console.log(`Received MP4 Download generation request for ${payload.uid}.`);
      const dlRes = await enableMP4Download(payload, env);
      success = dlRes >= 200 && dlRes < 300;
      break;
    case 'enableAutoCaptionsEN':
      console.log(`Received auto-generated captions request for ${payload.uid}.`);
      const capReq = await enableAutoCaptions(payload, env);
      success = capReq >= 200 && capReq < 300;
      break;
    default:
      console.log(`Unknown action requested: ${payload}`);
      success = true;
      break;
  }

  if (success) {
    console.log('Success, acknowledging message');
    message.ack();
  } else {
    console.log('Failed; will retry in 1 minute');
    // Delay for 1 minutes and try again...
    message.retry({ delaySeconds: 60 });
  }
}
