import express from 'express';
import cors from 'cors';

import dataApi from './data';
import { runAsyncWrapper, randomString, isObject } from './utils';

const corsConfig = cors({
  origin: '*',
  // Set the Access-Control-Max-Age header to 365 days.
  maxAge: 60 * 60 * 24 * 365,
});

const app = express();
app.use(corsConfig);
app.use(express.json());

app.get('/', (_req, res) => {
  res.send('Welcome to <a href="https://augurrank.com">AugurRank tasker</a>\'s server!');
});

app.post('/run', runAsyncWrapper(async (req, res) => {
  const logKey = randomString(12);
  console.log(`(${logKey}) /run receives a post request`);

  const reqBody = req.body;
  console.log(`(${logKey}) Request body: ${JSON.stringify(reqBody)}`);
  if (!isObject(reqBody)) {
    console.log(`(${logKey}) Invalid reqBody, just end`);
    res.status(200).end();
    return;
  }

  const { oldUser, newUser, oldPred, newPred } = reqBody;
  if (oldUser !== null && !isObject(oldUser)) {
    console.log(`(${logKey}) Invalid oldUser, just end`);
    res.status(200).end();
    return;
  }
  if (!isObject(newUser)) {
    console.log(`(${logKey}) Invalid newUser, just end`);
    res.status(200).end();
    return;
  }
  if (oldPred !== null && !isObject(oldPred)) {
    console.log(`(${logKey}) Invalid oldPred, just end`);
    res.status(200).end();
    return;
  }
  if (!isObject(newPred)) {
    console.log(`(${logKey}) Invalid newPred, just end`);
    res.status(200).end();
    return;
  }

  await dataApi.updateTotal(oldUser, newUser, oldPred, newPred);

  console.log(`(${logKey}) Finish`);
  res.status(200).end();
}));

// Listen to the Cloud Run-specified port, or 8088 otherwise
const PORT = process.env.PORT || 8088;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}...`);
  console.log('Press Ctrl+C to quit.');
});
