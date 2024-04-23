/**
 * @file outbound.ts
 *
 * Functions that make external requests to the Stream API
 */

import { Env, SecondaryOpRequestMessage, UploadRequestMessage } from ".";

/**
 * Send a request to Stream to fetch a video from URL
 *
 * @param payload
 * @param env
 */
export const uploadFetch = async (payload: UploadRequestMessage, env: Env): Promise<number> => {
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
  return res.status;
};

/**
 * Send a request to Stream to enable MP4 downloads for a video
 *
 * @param payload
 * @param env
 * @returns
 */
export const enableMP4Download = async (payload: SecondaryOpRequestMessage, env: Env): Promise<number> => {
  const res = await fetch(`${env.CF_API}/${env.CF_ACCT_TAG}/stream/${payload.uid}/downloads`, {
    headers: {
      'Authorization': `Bearer ${env.CF_STREAM_KEY}`,
    },
    method: 'POST',
  });

  console.log(`Stream responded ${res.status} ${res.statusText}: \n${JSON.stringify(await res.json())}`);
  return res.status;
};

export const enableAutoCaptions = async (payload: SecondaryOpRequestMessage, env: Env) => {
  // @TODO: This should come in the payload eventually...
  const lang = 'en';

  const res = await fetch(`${env.CF_API}/${env.CF_ACCT_TAG}/stream/${payload.uid}/captions/${lang}/generate`, {
    headers: {
      'Authorization': `Bearer ${env.CF_STREAM_KEY}`,
    },
    method: 'POST',
  });

  console.log(`Stream responded ${res.status} ${res.statusText}: \n${JSON.stringify(await res.json())}`);
  return res.status;
};
