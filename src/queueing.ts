/**
 * @file queueing.ts
 *
 * Functions that handle reading/processing queue messages.
 */

import { Env, SecondaryOpRequestMessage, UploadRequestMessage } from ".";

export const processMessage = async (message: Message, env: Env): Promise<void> => {
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
