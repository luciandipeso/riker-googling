const bunyan = require('bunyan')
const compression = require('compression')
const express = require('express')
const serveStatic = require('serve-static')
const Database = require('better-sqlite3')
const validator = require('validator')
const fs = require('fs')
const path = require('path')
const twitter = require('twitter')
const bigInt = require('big-integer')
const nconf = require('nconf')

nconf.argv()
     .env()

const env = ["prod", "dev"].includes(nconf.get('NODE_ENV')) ? nconf.get('NODE_ENV') : "dev"
nconf.file({ file: path.join(__dirname, "conf", env + ".conf.json") })

/**
 * Logging
 */
var log = bunyan.createLogger({
  name: 'riker-googling', 
  version: nconf.get("version"), 
  port: nconf.get("serverPort"),
  streams: [
    {
      level: 'info',
      stream: process.stdout   
    },
    {
      level: 'warn',
      path: __dirname + "/logs/app.log"
    }
  ]
})

/**
 * Cache of Riker tweets
 */
const dbFile = path.join(__dirname, nconf.get('dbPath'), nconf.get('dbFile')) || "riker.db"
const db = new Database(dbFile, { readonly: true })

/**
 * UI Server
 */
var server = express()
server.use(compression())
server.listen(nconf.get("serverPort") || 3000)

/**
 * Twitter client
 */
var client = new twitter({
  consumer_key: nconf.get("twitter").consumerKey,
  consumer_secret: nconf.get("twitter").consumerSecret,
  access_token_key: nconf.get("twitter").accessToken,
  access_token_secret: nconf.get("twitter").accessSecret
})

// Rebuild Riker tweets
function rebuildCache(maxId, tweets, tries, sinceId) {
  var sinceId = sinceId || "433810184817750017",
      maxId = maxId || false,
      lastMaxId = maxId,
      bigIntMaxId = false,
      tries = tries || 0,
      tweets = tweets || {};

  if(maxId) {
    bigIntMaxId = bigInt(maxId);
  }

  var queryObj = { 
    screen_name: "RikerGoogling", 
    since_id: sinceId,
    trim_user: 1,
    exclude_replies: 1,
    include_rts: 0,
    count: 200
  }

  client.get('statuses/user_timeline.json', queryObj, function(err, params, response) {
    log.info({ sinceId: sinceId, maxId: maxId }, "Rebuilding cache")
    if(err) {
      if(tries >= 3) {
        // Stop
        log.error({ sinceId: sinceId, maxId: maxId, tweets: tweets, err: err }, "Failed rebuilding cache after 3 tries")
        return
      }
      log.warn({ sinceId: sinceId, maxId: maxId, err: err }, "Error querying Twitter")
      return rebuildCache(maxId, tweets, tries+1, sinceId)
    }

    if(!params.length) {
      // We're done fetching
      return fillCache(tweets);
    } else {
      for(var i=0, length=params.length; i<length; i++) {
        if(sinceId == params[i].id_str) {
          // We're done
          return fillCache(tweets)
        }

        if(!params[i].text || !params[i].text.length) {
          continue
        }

        var tweet = {
          text: params[i].text,
          char0: "",
          char1: "",
          char2: ""
        }

        if(tweet.text.length >= 1) {
          tweet.char0 = tweet.text.substring(0,1)
        }
        if(tweet.text.length >= 2) {
          tweet.char1 = tweet.text.substring(1,2)
        }
        if(tweet.text.length >= 3) {
          tweet.char2 = tweet.text.substring(2,3)
        }

        tweets[params[i].id_str] = tweet;

        if(!bigIntMaxId || bigIntMaxId.compare(params[i].id_str) > 0) {
          maxId = params[i].id_str
        }
      }
    }

    if(maxId && maxId == lastMaxId) {
      // We're done
      return fillCache(tweets)
    } else {
      // Fetch more
      return rebuildCache(maxId, tweets, 0, sinceId)
    }
  });
};

function fillCache(tweets) {
  log.info("Filling cache");
  const dbRw = new Database(dbFile)
  try {
    dbRw.prepare("DELETE FROM tweets").run()
  } catch(err) {
    log.error({ err: err }, "Unable to clear cache")
    return
  }
  
  dbRw.prepare("INSERT INTO tweets (t_id, tweet, char0, char1, char2) VALUES($id, $tweet, $char0, $char1, $char2);")

  var stmt = dbRw.prepare("INSERT INTO tweets (t_id, tweet, char0, char1, char2) VALUES($id, $tweet, $char0, $char1, $char2);")
  for(let i in tweets) {
    stmt.run({
      id: i,
      tweet: tweets[i].text,
      char0: tweets[i].char0,
      char1: tweets[i].char1,
      char2: tweets[i].char2
    })
  }
  dbRw.close();
}

rebuildCache();
setInterval(rebuildCache, 86400000)

// Routes
server.get('/tweets.json', function(req, res) {
  res.header('Content-Type', 'application/javascript');
  let tweets = [], rows = []

  try{
    rows = db.prepare("SELECT tweet FROM tweets").all()
  } catch(err) {
    log.warn({ err: err }, "Unable to query cache.");
    return res.status(500).jsonp(err);
  }

  tweets = rows
  return res.jsonp(tweets)
});

var oneDay = 24*60*60*1000;
server.use('/', serveStatic(path.normalize(__dirname + '/public'), { maxAge: oneDay }));