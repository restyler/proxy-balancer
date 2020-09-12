const request = require('./request.js');
const Bottleneck = require('bottleneck');
const ProxyAgent = require('simple-proxy-agent');
const utils = require('./utils.js');
const fs = require('fs');
const url = require('url');
const bunyan = require('bunyan');
const log = bunyan.createLogger({
  name: 'myapp',
  streams: [
    {
      level: 'info',
      stream: process.stdout            // log INFO and above to stdout
    },
    {
      level: 'debug',
      path: 'balancer-debug.log'            // log INFO and above to stdout
    },
    {
      level: 'error',
      path: 'balancer-error.log'  // log ERROR and above to a file
    }
  ]
});

const reqlog = bunyan.createLogger({
  name: 'req-logger',
  streams: [{
    level: 'info',
    path: 'balancer-requests.log'
  }]
});

const defaultOptions = {
  poolExpired: 1 * 360 * 1000,
  proxyFn: () => [],
  maxConcurrent: 25,
  minTime: 100,
  timeout: 5 * 1000,
  proxyTimeout: 2 * 1000,
  maxRetries: 5,
  fallbackProxy: undefined,
  statsStackSize: 10
}

class Balancer {
  constructor(options) {
    this.lastUpdate;
    this.proxies = [];
    this.proxyStats = {};
    this.next = 0;
    this.options = Object.assign({}, defaultOptions, options);
    this.limiter = new Bottleneck({
      maxConcurrent: options.maxConcurrent || defaultOptions.maxConcurrent,
      minTime: options.minTime || defaultOptions.minTime
    });
    this.fetching = false;
  }

  getStats() {
    let stats = Object.keys(this.proxyStats).map((k) => {
      return {
        addr: k,
        updatedAt: this.proxyStats[k].updatedAt,
        fails: this.proxyStats[k].fails,
        reqs: this.proxyStats[k].reqs.length
      }
    })

    return { stats, proxies: this.proxies }
  }

  getRawStats() {
    return this.proxyStats
  }

  async getProxies() {
    if(this.fetching) {
      if(this.proxies.length > 0) {
        return this.proxies;
      }
      await utils.delay(200);
      return this.getProxies();
    }

    if(!this.lastUpdate || this.proxies.length === 0 || Date.now() > this.lastUpdate + this.options.poolExpired) {
      this.fetching = true;
      try {
        const proxies = await this.options.proxyFn();
        this.proxies = proxies || [];
        this.lastUpdate = Date.now();
        this.fetching = false;
      } catch (err) {
        log.error(err)
      }
      
      
    }

    return this.proxies;
  }
  trimPassword(proxy) {
    if (proxy.includes('@')) {
      let proxyParts = url.parse(proxy);
      proxyParts.auth = proxyParts.auth.slice(0, proxyParts.auth.indexOf(':')) + '***';
      proxy = url.format(proxyParts);
    }

    return proxy
  }

  async logResult(url, proxy, result, time, retry) {
    let d = new Date();
    let dateIso = d.toISOString();
    let dateUnix = d.getTime();

    proxy = this.trimPassword(proxy)

    reqlog.info({
      c: dateIso, url, proxy, result, time, retry
    });
    if (typeof this.proxyStats[proxy] == 'undefined') {
      this.proxyStats[proxy] = { reqs: [], fails: 0, createdAt: dateIso };
    }
    this.proxyStats[proxy].updatedAt = dateIso;
    this.proxyStats[proxy].updatedAtTimestamp = dateUnix;
    this.proxyStats[proxy].reqs.push([dateUnix, result, url]);
    if (!result) {
      this.proxyStats[proxy].fails++
    }
    
    // poor mans stack implementation
    if (this.proxyStats[proxy].reqs.length > this.options.statsStackSize) {
      let removedElement = this.proxyStats[proxy].reqs.pop();
      if (!removedElement[1]) { // check result of the removed request
        this.proxyStats[proxy].fails--
      }
    }

    
  }

  async request(url, options, timeout = this.options.timeout / 1000, retry = 0) {
    let next = false;
    const startMs = Date.now();
    try {
      let lastTry = retry == this.options.maxRetries;
      next = await this.getNext(lastTry);

      const agent = new ProxyAgent(next, {
        timeout: this.options.proxyTimeout
      });
      log.debug('Trying agent:' + next);
      const res = await this.limiter.schedule(() => {
        log.debug('Inside req', { url, options, retry, proxy: next });
        return request(url, {
          agent: agent,
          ...options
        }, timeout)
      })

      // unfortunately, res.clone() is not working properly in node-fetch v2
      // so we have to resolve body here to check for contents
      res.resolvedText = await res.text();


      if (typeof options.textExpected !== 'undefined') {
        if (!res.resolvedText.includes(options.textExpected)) {
          throw new Error('Expected text not found');
        }
      }

      if (typeof options.textNotExpected !== 'undefined') {
        if (res.resolvedText.includes(options.textNotExpected)) {
          throw new Error('Not expected text found');
        }
      }

      this.logResult(url, next, true, Date.now() - startMs, retry);
      
      return res;
    } catch (err) {
      // Retry if proxy error
      log.error(err);
      this.logResult(url, next, false, Date.now() - startMs, retry);
      if (retry >= this.options.maxRetries) {
        throw err;
      }
      return this.request(url, options, timeout, retry + 1);
    }
  }

  async getNext(lastTry) {
    if (lastTry && typeof this.options.fallbackProxy !== 'undefined') {
      return this.options.fallbackProxy;
    }
    const proxies = await this.getProxies();
    if(proxies.length === 0) {
      throw new Error("Empty proxy list");
    }
    const proxy = proxies[this.next];
    if(proxy) {
      this.next = this.next + 1;
      return proxy;
    }
    this.next = 0;
    return proxies[0];
  }
}

module.exports = Balancer
