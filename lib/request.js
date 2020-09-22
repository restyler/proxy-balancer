const fetch = require('node-fetch');

async function request(url, options, timeout = 5) {
  try {
    const res = await fetch(url, {
      ...options,
      timeout: timeout * 1000
    })
    if(!res.ok) {
      if (res.status < 500) {
        return res;
      }
      throw new Error('Invalid Response');
    }
    return res;
  } catch (err) {
    throw err;
  }
}

module.exports = request;
