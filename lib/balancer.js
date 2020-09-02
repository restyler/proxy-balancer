const request = require('./request.js');
const Bottleneck = require('bottleneck');
const ProxyAgent = require('simple-proxy-agent');
const utils = require('./utils.js');
const fs = require('fs');
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
    return Object.keys(this.proxyStats).map((k) => {
      return {
        addr: k,
        fails: this.proxyStats[k].fails,
        reqs: this.proxyStats[k].reqs.length
      }
    })
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

  async logResult(url, proxy, result, time, retry) {
    let d = new Date();
    reqlog.info({
      c: d.toISOString(), url, proxy, result, time, retry
    });
    if (typeof this.proxyStats[proxy] == 'undefined') {
      this.proxyStats[proxy] = { reqs: [], fails: 0 };
    }
    this.proxyStats[proxy].reqs.push([d.getTime(), result, url]);
    if (!result) {
      this.proxyStats[proxy].fails++
    }
    
    // poor mans stack implementation
    if (this.proxyStats[proxy].reqs.length > this.options.statsStackSize) {
      let removedElement = this.proxyStats[proxy].pop();
      if (!removedElement[1]) { // check result of the removed request
        this.proxyStats[proxy].fails--
      }
    }

    
  }

  async request(url, options, timeout = this.options.timeout / 1000, retry = 0) {
    let next = false;
    const startMs = Date.now();
    try {
      next = await this.getNext();
      
      const agent = new ProxyAgent(next, {
        timeout: this.options.proxyTimeout
      });
      log.debug('Trying agent:' + next);
      const res = await this.limiter.schedule(() => {
        log.debug('Inside req', { url, options, retry })//! Retry: %d Requesting url: %s' + url + ' with opts:' + JSON.stringify(options), retry);
        return request(url, {
          agent: agent,
          ...options
        }, timeout)
      })
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

  async getNext() {
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
