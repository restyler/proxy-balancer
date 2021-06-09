const request = require('./request.js');
const Bottleneck = require('bottleneck');
const ProxyAgent = require('proxy-agent');
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
  healthCheckMinReqNum: 30,
  healthCheckMaxFailRate: 0.8,
  fallbackProxy: undefined,
  statsStackSize: 50,
  statsCleanupTimeout: 1 * 60 * 60 * 1000
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
        skips: this.proxyStats[k].skips,
        reqs: this.proxyStats[k].reqsNum
      }
    })

    return { stats, proxies: this.proxies.map((a) => this.trimPassword(a) ) }
  }

  // check if proxy has good stats
  proxyIsHealthy(addr) {
    //console.log('proxy health check', addr);

    addr = this.trimPassword(addr)

    let proxyStats = this.proxyStats[addr];

    //console.log('this.proxyStats[addr]', this.proxyStats[addr])

    // new proxy, healthy by defaul
    if (!proxyStats) {
      return true;
    }

    // consider all http proxies as good
    if (addr.startsWith('http')) {
      //return true;
    }

    // not enough stats
    if (proxyStats.reqsNum < this.options.healthCheckMinReqNum) {
      return true;
    }

    // consider proxies failing 90% of cases as bad
    console.log(`proxyIsHealthy health: ${addr}, fails: ${proxyStats.fails}, fail rate: ${proxyStats.fails / proxyStats.reqsNum}`)
    return (proxyStats.fails / proxyStats.reqsNum) < this.options.healthCheckMaxFailRate;

  }

  proxyStatsSkip(addr) {
    addr = this.trimPassword(addr)

    if (this.proxyStats[addr]) {
      this.proxyStats[addr].skips++;
    }

  }

  getRawStats() {
    return this.proxyStats
  }

  // helper utility for stats cleanup
  filterObject(obj, predicate) {
    return Object.keys(obj)
      .filter( key => predicate(obj[key]) )
      .reduce( (res, key) => (res[key] = obj[key], res), {} );
  }

  // @TODO: this needs testing!
  async cleanupStats() {
    this.proxyStats = this.filterObject(this.proxyStats, (val) => {
      // @TODO: implement separate option for fine-grained stats cleanup period? 
      return Date.now() < (val.updatedAtTimestamp + this.options.statsCleanupTimeout)
    })
  }

  async getProxies() {
    if(this.fetching) {
      if(this.proxies.length > 0) {
        return this.proxies;
      }
      await utils.delay(200);
      return this.getProxies();
    }

    if (Date.now() > this.lastUpdate + this.options.poolExpired) {
      await this.cleanupStats();
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
      this.proxyStats[proxy] = { reqs: [], reqsNum: 0, fails: 0, skips: 0, createdAt: dateIso };
    }
    this.proxyStats[proxy].reqsNum++;
    this.proxyStats[proxy].updatedAt = dateIso;
    this.proxyStats[proxy].updatedAtTimestamp = dateUnix;
    this.proxyStats[proxy].reqs.push([dateUnix, result, url, time]);
    if (!result) {
      this.proxyStats[proxy].fails++
    }
    
    // poor mans stack implementation
    if (this.proxyStats[proxy].reqs.length > this.options.statsStackSize) {
      this.proxyStats[proxy].reqs.shift();
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

      // @TODO: move 404 to dynamic config
      if (typeof options.textExpected !== 'undefined' && res.status != 404) {
        let textExpected = options.textExpected;
        if (!Array.isArray(textExpected)) {
          textExpected = [textExpected];
        }
         
        let foundOne = textExpected.some((textToSearch) => {
          if (res.resolvedText.includes(textToSearch)) {
            return true;
          }
        })

        if (!foundOne) {
          throw new Error('Expected text not found');
        }
      }
      // @TODO: move 404 to dynamic config
      if (typeof options.textNotExpected !== 'undefined' && res.status != 404) {
        let textNotExpected = options.textNotExpected;
        if (!Array.isArray(textNotExpected)) {
          textNotExpected = [textNotExpected];
        }

        textNotExpected.forEach((textToSearch) => {
          if (res.resolvedText.includes(textToSearch)) {
            throw new Error('Not expected text found');
          }
        })

        
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

      if (!this.proxyIsHealthy(proxy)) {
        this.proxyStatsSkip(proxy);
        return this.getNext();
      }

      
      return proxy;
    }
    this.next = 0;
    return proxies[0];
  }
}

module.exports = Balancer
