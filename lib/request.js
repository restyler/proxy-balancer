const fetch = require('node-fetch');
const AbortController = require('abort-controller');

async function request(url, options, timeout = 5) {
  let controller = new AbortController();
  let timeoutHandle = setTimeout(() => {
    controller.abort();
  }, (timeout * 1000) + 5000);


  try {
    const res = await fetch(url, {
      ...options,
      timeout: timeout * 1000,
      signal: controller.signal
    })
    if(!res.ok) {
      if (res.status < 500 && res.status != 403) {
        return res;
      }
      
      throw new Error('Invalid Response from upstream: ' + res.status + ' ' + res.statusText);
    }
    return res;
  } catch (err) {
    throw err;
  } finally {
    clearTimeout(timeoutHandle);
  }
}

module.exports = request;
