const request = require('./request.js');
const Bottleneck = require('bottleneck');
const ProxyAgent = require('simple-proxy-agent');
const utils = require('./utils.js');
const fs = require('fs');

const defaultOptions = {
  poolExpired: 1 * 60 * 1000,
  proxyFn: () => [],
  maxConcurrent: 15,
  minTime: 100,
  timeout: 4 * 1000,
  proxyTimeout: 2 * 1000,
  maxRetries: 1
}

class Balancer {
  constructor(options) {
    console.log('test!');
    this.lastUpdate;
    this.proxies = [];
    this.next = 0;
    this.options = Object.assign({}, defaultOptions, options);
    this.limiter = new Bottleneck({
      maxConcurrent: options.maxConcurrent || defaultOptions.maxConcurrent,
      minTime: options.minTime || defaultOptions.minTime
    });
    this.fetching = false;
  }

  async getProxies() {
    //console.log('getProxies call');
    if(this.fetching) {
      console.log('getProxies already fetching, skipping...');
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
        console.error(err)
      }
      
      
    } else {
      //console.log('getProxies lastUpdate empty or too fresh file, skipping...');
      //console.log('this.lastUpdate:' + this.lastUpdate);
      //console.log('this.options.poolExpired:' + this.options.poolExpired);
    }

    return this.proxies;
  }

  async logResult(url, proxy, result, time, attemptCounter) {
    if (!this.logFileStream) {
      this.logFileStream = fs.createWriteStream("proxy-req.log", {flags:'a'});
    }
    let nowIso = new Date().toISOString();

    this.logFileStream.write(nowIso + '|' + proxy + '|' + (result ? '1' : 0) + '|' + time + '|' + url + '|' + attemptCounter + "\n", function (err) {
      if (err) return console.log(err);
      console.log('Appended!');
    });
    
  }

  async request(url, options, timeout = this.options.timeout / 1000, retry = 0) {
    let next = false;
    const startMs = Date.now();
    try {
      next = await this.getNext();
      
      const agent = new ProxyAgent(next, {
        timeout: this.options.proxyTimeout
      });
      console.log('agent:' + next);
      const res = await this.limiter.schedule(() => {
        console.log('Inside! Retry: %d Requesting url:' + url + ' with opts:' + JSON.stringify(options), retry);
        return request(url, {
          agent: agent,
          ...options
        }, timeout)
      })
      this.logResult(url, next, true, Date.now() - startMs, retry);
      return res;
    } catch (err) {
      // Retry if proxy error
      console.error(err);
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
